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

import edu.ucla.cs.scai.clubsp.messages.ClubsPMessage;
import edu.ucla.cs.scai.clubsp.messages.ComputeBestSplitRequest;
import edu.ucla.cs.scai.clubsp.messages.ComputeValleyCriterionRequest;
import edu.ucla.cs.scai.clubsp.messages.DummyMessage;
import edu.ucla.cs.scai.clubsp.messages.ExecutionMessage;
import edu.ucla.cs.scai.clubsp.messages.FinalRefinementRequest;
import edu.ucla.cs.scai.clubsp.messages.GenerateDataSetRequest;
import edu.ucla.cs.scai.clubsp.messages.InitRootRequest;
import edu.ucla.cs.scai.clubsp.messages.IntermediateRefinementRequest;
import edu.ucla.cs.scai.clubsp.messages.LoadDataSetRequest;
import edu.ucla.cs.scai.clubsp.messages.ReceiveMarginalsRequest;
import edu.ucla.cs.scai.clubsp.messages.RestrictedCountRequest;
import edu.ucla.cs.scai.clubsp.messages.SendMarginalsRequest;
import edu.ucla.cs.scai.clubsp.messages.SplitRequest;
import java.io.ObjectInputStream;
import java.util.Date;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
class WorkerIncomingMessageHandler extends Thread {

    ObjectInputStream in;
    Worker worker;
    String senderName;

    public WorkerIncomingMessageHandler(ObjectInputStream in, Worker worker, String senderName) {
        this.in = in;
        this.worker = worker;
        this.senderName = senderName;
    }

    @Override
    public void run() {
        boolean canrun=true;
        while (canrun) {
            try {
                while (in.available()==0) {
                    System.out.println("Nothing available to read");
                    wait(100);
                }
                ClubsPMessage msg = (ClubsPMessage) in.readObject();
                System.out.println(System.currentTimeMillis() + ": Received command " + msg + " " + msg.getId());
                if (msg instanceof LoadDataSetRequest) {
                    LoadDataSetRequest c = (LoadDataSetRequest) msg;
                    worker.initExecution(c.executionId, c.dataSetId, c.workers, c.scaleFactor);
                } else if (msg instanceof GenerateDataSetRequest) {
                    GenerateDataSetRequest c = (GenerateDataSetRequest) msg;
                    worker.doGeneration(c.nOfTuples, c.domainWidth, c.noiseRatio, c.centers, c.radii);
                } else if (msg instanceof ExecutionMessage) {
                    WorkerExecution ex = worker.workerExecutions.get(((ExecutionMessage) msg).executionId);
                    if (msg instanceof InitRootRequest) {
                        InitRootRequest c = (InitRootRequest) msg;
                        ex.initRoot(c.globalDomain);
                    } else if (msg instanceof SendMarginalsRequest) {
                        SendMarginalsRequest c = (SendMarginalsRequest) msg;
                        ex.sendMarginals(c.blockId, c.dimension, c.receiverId);
                    } else if (msg instanceof ReceiveMarginalsRequest) {
                        ReceiveMarginalsRequest c = (ReceiveMarginalsRequest) msg;
                        ex.receiveMarginals(c.blockId, c.dimension, c.marginals, System.currentTimeMillis() - c.time);
                    } else if (msg instanceof ComputeBestSplitRequest) {
                        ComputeBestSplitRequest c = (ComputeBestSplitRequest) msg;
                        ex.computeBestSplit(c.blockId, c.dimension, c.globalN, c.globalLS, c.globalSS);
                    } else if (msg instanceof SplitRequest) {
                        SplitRequest c = (SplitRequest) msg;
                        ex.doSplit(c.blockId, c.splitDimension, c.splitPosition, c.leftId, c.rightId, c.globalN, c.globalLS, c.globalSS);
                    } else if (msg instanceof ComputeValleyCriterionRequest) {
                        ComputeValleyCriterionRequest c = (ComputeValleyCriterionRequest) msg;
                        ex.computeValleyCriterion(c.blockId, c.dimension, c.delta);
                    } else if (msg instanceof RestrictedCountRequest) {
                        RestrictedCountRequest c = (RestrictedCountRequest) msg;
                        ex.computeRestrictedCount(c.blockIds, c.restrictedRanges);
                    } else if (msg instanceof IntermediateRefinementRequest) {
                        IntermediateRefinementRequest c = (IntermediateRefinementRequest) msg;
                        ex.doIntermediateRefinement(c.clusterBlockIds, c.centroids, c.detailedRadii);
                    } else if (msg instanceof FinalRefinementRequest) {
                        FinalRefinementRequest c = (FinalRefinementRequest) msg;
                        ex.doFinalRefinement(c.centroids, c.detailedRadii);
                    } else {
                        System.out.println("Unrecognized message type");
                    }
                } else if (msg instanceof DummyMessage) {
                } else {
                    System.out.println("Unrecognized message type");
                }
            } catch (Exception e) {
                System.out.println("Error reading from the InputStream written by "+senderName+": " + e);
                e.printStackTrace();
            }            
        }    
        System.out.println("Error: a WorkerMessageHandler stopped working!!!");
    }
}
