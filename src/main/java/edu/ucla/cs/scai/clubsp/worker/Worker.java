/*
 * Copyright 2015 ScAi, CSD, UCLA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucla.cs.scai.clubsp.worker;

import edu.ucla.cs.scai.clubsp.commons.RegisteredWorker;
import edu.ucla.cs.scai.clubsp.messages.ClubsPMessage;
import edu.ucla.cs.scai.clubsp.messages.DummyMessage;
import edu.ucla.cs.scai.clubsp.messages.GenerateDataSetRequest;
import edu.ucla.cs.scai.clubsp.messages.WorkerConnectionRequest;
import edu.ucla.cs.scai.clubsp.messages.WorkerConnectionRequest2;
import edu.ucla.cs.scai.clubsp.messages.WorkerConnectionResponse;
import edu.ucla.cs.scai.clustering.syntheticgenerator.MultidimensionalGaussianGenerator;
import edu.ucla.cs.scai.clustering.syntheticgenerator.Range;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class Worker {

    int port;
    String datasetsPath;
    ObjectOutputStream masterOutputStream;
    final HashMap<String, WorkerExecution> workerExecutions = new HashMap<>();
    HashMap<String, ObjectOutputStream> workerOutputStreams = new HashMap<>();
    HashMap<String, RegisteredWorker> registeredWorkers = new HashMap<>();
    String ip;
    String id;
    String masterIp;
    int masterPort;

    public Worker(int port, String datasetsPath, String masterIp, int masterPort) throws Exception {
        this.port = port;
        File f = new File(datasetsPath);
        if (!f.exists() || !f.isDirectory()) {
            System.out.println("Directory " + datasetsPath + " not found");
            System.out.println("Worker terminated");
            throw new Exception("Wrong path " + datasetsPath);
        }
        this.datasetsPath = datasetsPath;
        if (!datasetsPath.endsWith(File.pathSeparator)) {
            datasetsPath += File.pathSeparator;
        }
        this.masterIp = masterIp;
        this.masterPort = masterPort;
    }

    public void start() throws Exception {
        //start the deadlock detection
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
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
        try (ServerSocket listener = new ServerSocket(port, 1000);) {
            System.out.println("Worker started. Waiting for an id");
            //register with the master
            try {
                Socket socketOut = new Socket(masterIp, masterPort);
                socketOut.setTcpNoDelay(true);
                socketOut.setKeepAlive(true);
                masterOutputStream = new ObjectOutputStream(socketOut.getOutputStream());
                System.out.println("Saved socket to send messages to master");
                masterOutputStream.writeObject(new WorkerConnectionRequest(port));
            } catch (Exception e) {
                System.out.println("Connection to master " + masterIp + ":" + masterPort + " failed");
                e.printStackTrace();
                System.out.println("Worker terminated");
                System.exit(0);
            }
            while (true) {
                Socket socketIn = listener.accept();
                socketIn.setTcpNoDelay(true);
                try {
                    ObjectInputStream in = new ObjectInputStream(socketIn.getInputStream());
                    ClubsPMessage msg = (ClubsPMessage) in.readObject();
                    //System.out.println("Received message " + msg);
                    if (msg instanceof WorkerConnectionResponse) {
                        socketIn.setTcpNoDelay(true);
                        socketIn.setKeepAlive(true);
                        WorkerConnectionResponse res = (WorkerConnectionResponse) msg;
                        this.id = res.assignedId;
                        System.out.println("Received id: " + this.id);
                        new WorkerIncomingMessageHandler(in, this).start();
                        System.out.println("Started listener on socket from master");
                        //don't close anything
                    } else if (msg instanceof WorkerConnectionRequest2) {
                        socketIn.setTcpNoDelay(true);
                        socketIn.setKeepAlive(true);
                        WorkerConnectionRequest2 res = (WorkerConnectionRequest2) msg;
                        new WorkerIncomingMessageHandler(in, this).start();
                        System.out.println("Started listener on socket from worker " + res.id);
                        //don't close anything
                    } else {
                        System.out.println("Unrecognized message type");
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println(e);
                    try {
                        //close socket
                        socketIn.close();
                    } catch (IOException ex) {
                        Logger.getLogger(Worker.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Worker terminated");
        }
    }

    public synchronized void sendMessageToMaster(ClubsPMessage message) {
        try {
            masterOutputStream.writeObject(message);
            masterOutputStream.flush();
            masterOutputStream.writeObject(new DummyMessage(message.getId()));
            masterOutputStream.flush();
        } catch (Exception e) {
            System.out.println("Error sending " + message + " to master\n" + e.getMessage());
            e.printStackTrace();
        }
    }

    public synchronized void sendMessageToWorker(String workerId, ClubsPMessage message) {
        try {
            System.out.println("Sending " + message + " to " + workerId);
            ObjectOutputStream oos = workerOutputStreams.get(workerId);
            oos.writeObject(message);
            oos.flush();
            oos.writeObject(new DummyMessage(message.getId()));
            oos.flush();
            System.out.println("Sent " + message + " to " + workerId);
        } catch (Exception e) {
            System.out.println("Error sending " + message + " to worker " + workerId + "\n" + e.getMessage());
            e.printStackTrace();
        }
    }

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public String getId() {
        return id;
    }

    //args[0] is the port used by this Worker
    //args[1] is the local path with the datasets
    //args[2] is the ip of the master
    //args[3] is the port of the master
    public static void main(String[] args) {
        if (args == null || args.length != 4) {
            args = new String[]{"" + (10000 + (int) (Math.random() * 10000)), "/home/massimo/", "localhost"/*"131.179.64.170"*/, "9192"};
            //args = new String[]{"" + (10000 + (int) (Math.random() * 10000)), "/home/massimo/", "131.179.64.145"/*"131.179.64.170"*/, "9090"};
            //System.out.println("Parameters needed: port dataSetsPath masterIp masterPort");
            //System.out.println("Worker terminated");
            //return;
        }
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.out.println("Port " + args[0] + " not valid");
            System.out.println("Worker terminated");
            return;
        }
        int masterPort;
        try {
            masterPort = Integer.parseInt(args[3]);
        } catch (Exception e) {
            System.out.println("Master port " + args[3] + " not valid");
            System.out.println("Worker terminated");
            return;
        }
        try {
            new Worker(port, args[1], args[2], masterPort).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initId(String assignedId, Exception e) {
        if (assignedId == null) {
            System.out.println("Registration failed: " + e.getMessage());
        } else {
            System.out.println("Received id: " + assignedId);
            id = assignedId;
        }
    }

    //start a new clustering execution
    public synchronized void initExecution(String executionId, String dataSetId, HashMap<String, RegisteredWorker> workers, double scaleFactor) {
        registeredWorkers.putAll(workers);
        for (RegisteredWorker worker : workers.values()) {
            if (!workerOutputStreams.containsKey(worker.id) && !worker.id.equals(id)) {
                try {
                    Socket socketOut = new Socket(worker.ip, worker.port);
                    socketOut.setTcpNoDelay(true);
                    socketOut.setKeepAlive(true);
                    ObjectOutputStream out = new ObjectOutputStream(socketOut.getOutputStream());
                    workerOutputStreams.put(worker.id, out);
                    System.out.println("Saved socket to send messages to worker " + worker.id);
                    out.writeObject(new WorkerConnectionRequest2(id));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        WorkerExecution newExec = new WorkerExecution(this, executionId, dataSetId, scaleFactor);
        workerExecutions.put(executionId, newExec);
    }

    public synchronized void doGeneration(int nOfTuples, int domainWidth, double noiseRatio, int[][] centers, int[][] radii) {
        int nOfClusters = centers.length;
        int dimensionality = centers[0].length;
        String fileName = datasetsPath + nOfTuples + "p_" + dimensionality + "d_" + nOfClusters + "c_" + noiseRatio + "n.data";
        System.out.println("Generating " + fileName);
        int[] inf = new int[dimensionality];
        int[] sup = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            inf[i] = 0;
            sup[i] = domainWidth - 1;
        }

        Range r = new Range(inf, sup);
        try {
            MultidimensionalGaussianGenerator.generate(fileName, r, nOfTuples, centers, radii, noiseRatio, 100);
            MultidimensionalGaussianGenerator.createImage(fileName, fileName + "_labels", r, true);
            String fileNameOut = datasetsPath + nOfTuples + "p_" + dimensionality + "d_" + nOfClusters + "c_" + noiseRatio + "n.data";
            MultidimensionalGaussianGenerator.shuffleDataset(fileName, fileName + "_labels", fileNameOut, fileNameOut + "_labels", new Random());
        } catch (Exception e) {
            e.printStackTrace();;
        }
    }
}
