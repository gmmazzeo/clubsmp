/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucla.cs.scai.clubsp.master;

import edu.ucla.cs.scai.clubsp.commons.RegisteredWorker;
import edu.ucla.cs.scai.clubsp.messages.ClubsPMessage;
import edu.ucla.cs.scai.clubsp.messages.DummyMessage;
import edu.ucla.cs.scai.clubsp.messages.GenerateDataSetRequest;
import edu.ucla.cs.scai.clubsp.messages.StartClusteringRequest;
import edu.ucla.cs.scai.clubsp.messages.StartGenerationRequest;
import edu.ucla.cs.scai.clubsp.messages.WorkerConnectionRequest;
import edu.ucla.cs.scai.clubsp.messages.WorkerConnectionResponse;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class Master {

    int port;
    final HashMap<String, ObjectOutputStream> workerOutputStreams = new HashMap<>(); //used to send messages to workers
    final HashMap<String, MasterExecution> masterExecutions = new HashMap<>();
    final HashMap<String, RegisteredWorker> registeredWorkers = new HashMap<>();

    public Master(int port) throws Exception {
        this.port = port;
    }

    //start listening on the port specified with the constructor
    public void start() throws Exception {
        //start the deadlock detection
        /*
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
                    long[] threadIds = bean.findDeadlockedThreads(); // Returns null if no threads are deadlocked.
                    if (threadIds != null) {
                        ThreadInfo[] infos = bean.getThreadInfo(threadIds);
                        for (ThreadInfo info : infos) {
                            StackTraceElement[] stack = info.getStackTrace();
                            System.out.println("Deadlock detected:");
                            for (StackTraceElement s : stack) {
                                System.out.println(s);
                            }
                        }
                        System.exit(0);
                    } else {
                        System.out.println("No deadlock detected");
                    }
                }
            }
        }.start();
        */
        try (ServerSocket listener = new ServerSocket(port, 1000);) {
            System.out.println("Master started at " + listener.getInetAddress().toString() + ":" + listener.getLocalPort());
            while (true) {
                Socket socketIn = listener.accept();
                socketIn.setTcpNoDelay(true);
                try {
                    ObjectInputStream in = new ObjectInputStream(socketIn.getInputStream());
                    ClubsPMessage msg = (ClubsPMessage) in.readObject();
                    System.out.println("Received command " + msg);
                    if (msg instanceof WorkerConnectionRequest) {
                        //when a worker connects to the master, it is associated with an id
                        String id = "w" + (workerOutputStreams.size() + 1);
                        System.out.println("Worker " + id + " registered");
                        socketIn.setTcpNoDelay(true);
                        socketIn.setKeepAlive(true);
                        socketIn.setSoTimeout(0);
                        //the socket the worker used to connect is associated with an handler
                        //only this socket will be used by the worker in order to send messages to the master
                        new MasterIncomingMessageHandler(in, this, id).start();
                        System.out.println("Started listener on socket from worker " + id);
                        String[] ip = socketIn.getInetAddress().toString().split("\\/");
                        WorkerConnectionRequest c = (WorkerConnectionRequest) msg;
                        //only this socket will be used by the master to send messages to the worker
                        Socket socketOut = new Socket(ip[ip.length - 1], c.port);
                        socketOut.setTcpNoDelay(true);
                        socketOut.setKeepAlive(true);
                        socketOut.setSoTimeout(0);
                        workerOutputStreams.put(id, new ObjectOutputStream(socketOut.getOutputStream()));
                        System.out.println("Saved socket to send messages to worker " + id);
                        sendMessage(id, new WorkerConnectionResponse(id));
                        registeredWorkers.put(id, new RegisteredWorker(id, ip[ip.length - 1], c.port));
                        //don't close anything
                    } else if (msg instanceof StartClusteringRequest) {
                        StartClusteringRequest c = (StartClusteringRequest) msg;
                        initExecution(c.dataSetId, 1);
                        //close socket
                        socketIn.close();
                    } else if (msg instanceof StartGenerationRequest) {
                        StartGenerationRequest c = (StartGenerationRequest) msg;
                        initGeneration(c.nOfTuples, c.dimensionality, c.domainWidth, c.nOfClusters, c.noiseRatio);
                        //close socket
                        socketIn.close();
                    } else {
                        System.out.println("Unrecognized message type");
                        //close socket
                        socketIn.close();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println(e);
                    try {
                        //close socket
                        socketIn.close();
                    } catch (IOException ex) {
                        Logger.getLogger(Master.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Master terminated");
        }
    }

    //send a message to a registered worker
    public synchronized void sendMessage(String workerId, ClubsPMessage message) {
        try {
            System.out.println("Sending " + message + " to " + workerId);
            ObjectOutputStream oos = workerOutputStreams.get(workerId);
            oos.writeObject(message);
            oos.flush();
            //oos.writeObject(new DummyMessage(message.getId()));
            //oos.flush();
            System.out.println("Sent " + message + " to " + workerId);
        } catch (Exception e) {
            System.out.println("Error sending " + message + " to worker " + workerId + "\n" + e.getMessage());
            e.printStackTrace();
        }
    }

    //start a new clustering execution
    public synchronized void initExecution(String dataSetId, double scaleFactor) {
        if (registeredWorkers.isEmpty()) {
            System.out.println("No workers available, sorry!");
        } else {
            MasterExecution newExec = new MasterExecution(this, dataSetId, scaleFactor);
            masterExecutions.put(newExec.executionId, newExec);
        }
    }

    //args[0] is the port used by the master
    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            args = new String[]{"9192"};
            //System.out.println("Parameters needed: port");
            //return;
        }
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.out.println("Port " + args[0] + " not valid");
            System.out.println("Master terminated");
            return;
        }
        try {
            new Master(port).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initGeneration(int nOfTuples, int dimensionality, int domainWidth, int nOfClusters, double noiseRatio) {
        Random rand = new Random(100);
        ArrayList<Double>[] positions = new ArrayList[dimensionality];
        double interval = 1.0 / (nOfClusters + 1);
        for (int dim = 0; dim < positions.length; dim++) {
            positions[dim] = new ArrayList<>();
            for (int clus = 0; clus < nOfClusters; clus++) {
                positions[dim].add((clus + 1) * interval - 0.05 * interval + 0.1 * interval * rand.nextDouble());
            }
            Collections.shuffle(positions[dim], rand);
        }
        int[][] centers = new int[nOfClusters][dimensionality];
        for (int i = 0; i < nOfClusters; i++) {
            for (int k = 0; k < dimensionality; k++) {
                centers[i][k] = (int) (domainWidth * positions[k].get(i));
            }
        }
        int radii[][] = new int[nOfClusters][dimensionality];
        for (int clus = 0; clus < nOfClusters; clus++) {
            for (int i = 0; i < dimensionality; i++) {
                radii[clus][i] = (int) (0.5 + domainWidth * (0.2 * interval + 0.8 * rand.nextDouble() * interval));
            }
        }
        for (String workerId : registeredWorkers.keySet()) {
            sendMessage(workerId, new GenerateDataSetRequest(nOfTuples, domainWidth, noiseRatio, centers, radii));
        }
    }
}
