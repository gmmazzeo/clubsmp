/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucla.cs.scai.clubsp.master;

import java.util.ArrayList;
import java.util.HashSet;

/**
 *
 * @author Giuseppe M. Mazzeo <mazzeo@cs.ucla.edu>
 */
public class MarginalComputationExecutionPlan {

    ArrayList<String>[] workerAllocations;

    HashSet<Integer> completedDimensions = new HashSet<>();
    int[] numberOfPairsLeft;

    public MarginalComputationExecutionPlan(int dimensionality, ArrayList<String> workerIds) {
        workerAllocations = new ArrayList[dimensionality];
        numberOfPairsLeft = new int[dimensionality];
        int n = workerIds.size();
        for (int i = 0; i < dimensionality; i++) {
            workerAllocations[i] = new ArrayList<>();
            for (int k = 0; k < n; k++) {
                workerAllocations[i].add(workerIds.get((i + k) % n));
            }
            if (n == 1) {
                completedDimensions.add(i);
            }
        }
    }

    public synchronized String[][] initComputation(int d) {        
        int n = workerAllocations[d].size();
        if (n == 1 || numberOfPairsLeft[d] > 0) { //this should never happen
            System.out.println("Unexpected situation at "+getClass().getName()+":initComputation");
            return null;
        }
        String[][] res = new String[n / 2][2];
        for (int i = 0; i < n - 1; i += 2) {
            //machines with odd index send marginal to machines with even index - index count starts from 0
            res[i / 2][0] = workerAllocations[d].get(i);
            res[i / 2][1] = workerAllocations[d].get(i + 1);
        }
        numberOfPairsLeft[d] = res.length;
        System.out.print("MCP for dimension "+d+": ");
        for (String[] re : res) {
            System.out.print(" " + re[0] + " <- " + re[1]);
        }
        System.out.println();
        return res;
    }

    public synchronized Boolean decreasePairsLeft(int d) { //returns true when the communication between all the pairs on one level of the tree is completed
        if (numberOfPairsLeft[d] == 0) {
            //an error occurred
            System.out.println("Unexpected situation at "+getClass().getName()+":decreasePairsLeft");
            return null;
        }
        numberOfPairsLeft[d]--;
        if (numberOfPairsLeft[d] == 0) { //the computation of the current level of the tree representing the MCP is complete
            //remove machines with odd index
            ArrayList<String> newMachineAllocation = new ArrayList<>();
            for (int i = 0; i < workerAllocations[d].size(); i++) {
                if (i % 2 == 0) {
                    newMachineAllocation.add(workerAllocations[d].get(i));
                }
            }
            workerAllocations[d] = newMachineAllocation;
            if (newMachineAllocation.size() == 1) {
                System.out.println("MCP completed for dimension "+d);
                completedDimensions.add(d);
            }
            return true;
        }
        return false;
    }
}