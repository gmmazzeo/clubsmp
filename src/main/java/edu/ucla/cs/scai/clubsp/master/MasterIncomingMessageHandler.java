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
package edu.ucla.cs.scai.clubsp.master;

import edu.ucla.cs.scai.clubsp.messages.ClubsPMessage;
import edu.ucla.cs.scai.clubsp.messages.ComputeBestSplitResponse;
import edu.ucla.cs.scai.clubsp.messages.ComputeValleyCriterionResponse;
import edu.ucla.cs.scai.clubsp.messages.DummyMessage;
import edu.ucla.cs.scai.clubsp.messages.ExecutionMessage;
import edu.ucla.cs.scai.clubsp.messages.FinalRefinementResponse;
import edu.ucla.cs.scai.clubsp.messages.InitRootResponse;
import edu.ucla.cs.scai.clubsp.messages.IntermediateRefinementResponse;
import edu.ucla.cs.scai.clubsp.messages.LoadDataSetResponse;
import edu.ucla.cs.scai.clubsp.messages.ReceiveMarginalsResponse;
import edu.ucla.cs.scai.clubsp.messages.RestrictedCountResponse;
import edu.ucla.cs.scai.clubsp.messages.SplitResponse;
import java.io.ObjectInputStream;
import java.util.Date;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
class MasterIncomingMessageHandler extends Thread {

    ObjectInputStream in;
    Master master;

    public MasterIncomingMessageHandler(ObjectInputStream in, Master master) {
        this.in = in;
        this.master = master;
    }

    @Override
    public void run() {
        while (true) {
            try {
                ClubsPMessage msg = (ClubsPMessage) in.readObject();
                System.out.println(System.currentTimeMillis() + ": Received command " + msg + " " + msg.getId());
                if (msg instanceof ExecutionMessage) {
                    MasterExecution ex = master.masterExecutions.get(((ExecutionMessage) msg).executionId);
                    if (msg instanceof LoadDataSetResponse) {
                        LoadDataSetResponse c = (LoadDataSetResponse) msg;
                        ex.increaseReceivedLocalDoamins(c.localDomain);
                    } else if (msg instanceof InitRootResponse) {
                        InitRootResponse c = (InitRootResponse) msg;
                        ex.increaseReceivedRootInfo(c.n, c.LS, c.SS);
                    } else if (msg instanceof ReceiveMarginalsResponse) {
                        ReceiveMarginalsResponse c = (ReceiveMarginalsResponse) msg;
                        ex.increaseReceivedMarginals(c.blockId, c.dimension, c.time);
                    } else if (msg instanceof ComputeBestSplitResponse) {
                        ComputeBestSplitResponse c = (ComputeBestSplitResponse) msg;
                        ex.increaseReceivedBestSplits(c.blockId, c.dimension, c.split);
                    } else if (msg instanceof ComputeValleyCriterionResponse) {
                        ComputeValleyCriterionResponse c = (ComputeValleyCriterionResponse) msg;
                        ex.increaseReceivedValleyCriterion(c.blockId, c.dimension, c.satisfied);
                    } else if (msg instanceof SplitResponse) {
                        SplitResponse c = (SplitResponse) msg;
                        ex.increaseReceivedSplit(c.blockId, c.split);
                    } else if (msg instanceof RestrictedCountResponse) {
                        RestrictedCountResponse c = (RestrictedCountResponse) msg;
                        ex.increaseReceivedRestrictedCount(c.blockIds, c.restrictedCount);
                    } else if (msg instanceof IntermediateRefinementResponse) {
                        IntermediateRefinementResponse c = (IntermediateRefinementResponse) msg;
                        ex.increaseReceivedIntermediateRefinement(c.blockIds, c.n, c.LS, c.SS);
                    } else if (msg instanceof FinalRefinementResponse) {
                        FinalRefinementResponse c = (FinalRefinementResponse) msg;
                        ex.increaseReceivedFinalRefinement(c.n, c.LS, c.SS);
                    } else {
                        System.out.println("Unrecognized message type");
                    }
                } else if (msg instanceof DummyMessage) {
                } else {
                    System.out.println("Unrecognized message type");
                }
            } catch (Exception e) {
                System.out.println("Error reading messare: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
