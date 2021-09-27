/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.ml.core.decisiontree;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;

import java.util.Stack;

public abstract class DecisionTree<L extends DecisionTreeLoss, P> {

    private final AllocationTracker allocationTracker;
    private final L lossFunction;
    private final HugeObjectArray<double[]> allFeatures;
    private final int maxDepth;
    private final int minSize;

    public DecisionTree(
        AllocationTracker allocationTracker,
        L lossFunction,
        HugeObjectArray<double[]> allFeatures,
        int maxDepth,
        int minSize
    ) {
        assert allFeatures.size() > 0;
        assert maxDepth >= 1;
        assert minSize >= 0;

        this.allocationTracker = allocationTracker;
        this.lossFunction = lossFunction;
        this.allFeatures = allFeatures;
        this.maxDepth = maxDepth;
        this.minSize = minSize;
    }

    public TreeNode<P> train() {
        var stack = new Stack<StackRecord<P>>();
        TreeNode<P> root;

        {
            var startGroup = HugeLongArray.newArray(allFeatures.size(), allocationTracker);
            startGroup.setAll(i -> i);
            root = splitAndPush(stack, startGroup, startGroup.size(), 1);
        }

        while (!stack.empty()) {
            var record = stack.pop();
            var split = record.split();

            if (record.depth() >= maxDepth || split.leftGroupSize() <= minSize) {
                record.node().leftChild = new LeafNode<>(toTerminal(split.leftGroup(), split.leftGroupSize()));
            } else {
                record.node().leftChild = splitAndPush(
                    stack,
                    split.leftGroup(),
                    split.leftGroupSize(),
                    record.depth() + 1
                );
            }

            if (record.depth() >= maxDepth || split.rightGroupSize() <= minSize) {
                record.node().rightChild = new LeafNode<>(toTerminal(split.rightGroup(), split.rightGroupSize()));
            } else {
                record.node().rightChild = splitAndPush(
                    stack,
                    split.rightGroup(),
                    split.rightGroupSize(),
                    record.depth() + 1
                );
            }
        }

        return root;
    }

    protected abstract P toTerminal(HugeLongArray group, long groupSize);

    private TreeNode<P> splitAndPush(Stack<StackRecord<P>> stack, HugeLongArray group, long groupSize, int depth) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert depth >= 1;

        var split = findBestSplit(group, groupSize);
        if (split.rightGroupSize() == 0) {
            return new LeafNode<>(toTerminal(split.leftGroup(), split.leftGroupSize()));
        } else if (split.leftGroupSize() == 0) {
            return new LeafNode<>(toTerminal(split.rightGroup(), split.rightGroupSize()));
        }

        var nonLeafNode = new NonLeafNode<P>(split.index(), split.value());
        stack.push(ImmutableStackRecord.of(nonLeafNode, split, depth));

        return nonLeafNode;
    }

    private long[] createSplit(
        final int index,
        final double value,
        HugeLongArray group,
        final long groupSize,
        HugeLongArray[] childGroups
    ) {
        assert groupSize > 0;
        assert group.size() >= groupSize;
        assert childGroups.length == 2;
        assert index >= 0 && index < allFeatures.get(0).length;

        long leftGroupSize = 0;
        long rightGroupSize = 0;

        for (int i = 0; i < groupSize; i++) {
            var featuresIdx = group.get(i);
            var features = allFeatures.get(featuresIdx);
            if (features[index] < value) {
                childGroups[0].set(leftGroupSize++, featuresIdx);
            } else {
                childGroups[1].set(rightGroupSize++, featuresIdx);
            }
        }

        return new long[]{leftGroupSize, rightGroupSize};
    }

    private Split findBestSplit(HugeLongArray group, final long groupSize) {
        assert groupSize > 0;
        assert group.size() >= groupSize;

        int bestIdx = -1;
        double bestValue = Double.MAX_VALUE;
        double bestLoss = Double.MAX_VALUE;

        HugeLongArray[] childGroups = {
            HugeLongArray.newArray(groupSize, allocationTracker),
            HugeLongArray.newArray(groupSize, allocationTracker)
        };
        HugeLongArray[] bestChildGroups = {
            HugeLongArray.newArray(groupSize, allocationTracker),
            HugeLongArray.newArray(groupSize, allocationTracker)
        };
        long[] bestGroupSizes = {-1, -1};

        for (int i = 0; i < allFeatures.get(0).length; i++) {
            for (int j = 0; j < groupSize; j++) {
                var features = allFeatures.get(group.get(j));

                var groupSizes = createSplit(i, features[i], group, groupSize, childGroups);

                var loss = lossFunction.splitLoss(childGroups, groupSizes);

                if (loss < bestLoss) {
                    bestIdx = i;
                    bestValue = features[i];
                    bestLoss = loss;

                    var tmpGroups = bestChildGroups;
                    bestChildGroups = childGroups;
                    childGroups = tmpGroups;

                    bestGroupSizes = groupSizes;
                }
            }
        }

        return ImmutableSplit.of(
            bestIdx,
            bestValue,
            bestChildGroups[0],
            bestChildGroups[1],
            bestGroupSizes[0],
            bestGroupSizes[1]
        );
    }

    @ValueClass
    interface Split {
        int index();

        double value();

        HugeLongArray leftGroup();

        HugeLongArray rightGroup();

        long leftGroupSize();

        long rightGroupSize();
    }

    @ValueClass
    interface StackRecord<P> {
        NonLeafNode<P> node();

        Split split();

        int depth();
    }

    static class LeafNode<P> implements TreeNode<P> {
        private final P prediction;

        LeafNode(P prediction) {
            this.prediction = prediction;
        }

        public P predict(double[] unused) {
            return this.prediction;
        }
    }

    static class NonLeafNode<P> implements TreeNode<P> {
        private final int index;
        private final double value;
        private TreeNode<P> leftChild;
        private TreeNode<P> rightChild;

        NonLeafNode(int index, double value) {
            assert index >= 0;

            this.index = index;
            this.value = value;
        }

        public P predict(double[] features) {
            assert features.length > this.index;
            assert this.leftChild != null;
            assert this.rightChild != null;

            if (features[this.index] < this.value) {
                return this.leftChild.predict(features);
            } else {
                return this.rightChild.predict(features);
            }
        }
    }
}
