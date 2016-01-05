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
package edu.ucla.cs.scai.clubsp.messages;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class SplitRequest extends ExecutionMessage {

    public int blockId;
    public int splitDimension;
    public int splitPosition;
    public int leftId, rightId;
    public int globalN;
    public double[] globalLS;
    public double[] globalSS;

    public SplitRequest(String executionId, int blockId, int splitDimension, int splitPosition, int leftId, int rightId, int globalN, double[] globalLS, double[] globalSS) {
        super(executionId);
        this.blockId = blockId;
        this.splitDimension = splitDimension;
        this.splitPosition = splitPosition;
        this.leftId = leftId;
        this.rightId = rightId;
        this.globalN = globalN;
        this.globalLS = globalLS;
        this.globalSS = globalSS;
    }

    @Override
    public String toString() {
        return "SplitRequest{" + "blockId=" + blockId + ", splitDimension=" + splitDimension + ", splitPosition=" + splitPosition + ", leftId=" + leftId + ", rightId=" + rightId + ", globalN=" + globalN + ", globalLS=" + globalLS + ", globalSS=" + globalSS + '}';
    }

}