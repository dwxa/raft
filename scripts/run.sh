for i in {1..1000}
do
    # Run the single test case
    # bash ../scripts/rafttest_single.sh testOneCandidateOneRoundElection

    # Run all test cases
    bash ../scripts/rafttest.sh
done