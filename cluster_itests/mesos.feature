Feature: Tron can connect to a mesos cluster

  Scenario: Framework registration
    Given a working mesos cluster
     When we run tronctl start MASTER.mesostest
      And we sleep 3 seconds
     Then we should see 1 frameworks
     Then we should see tron in the list of frameworks
