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

import edu.ucla.cs.scai.clubsp.commons.BestSplitResult;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class ComputeBestSplitResponse extends ExecutionMessage {

    public int blockId;
    public int dimension;
    public BestSplitResult split;

    public ComputeBestSplitResponse(String executionId, int blockId, int dimension, BestSplitResult split) {
        super(executionId);
        this.blockId = blockId;
        this.dimension = dimension;
        this.split=split;
    }

    @Override
    public String toString() {
        return "ComputeBestSplitResponse{" + "blockId=" + blockId + ", dimension=" + dimension + ", split=" + split + '}';
    }

}
