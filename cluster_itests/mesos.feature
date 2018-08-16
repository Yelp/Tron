Feature: Tron can connect to a mesos cluster

  Scenario: Framework registration
    Given a working mesos cluster
     Then we should see 1 frameworks
     Then we should see tron in the list of frameworks
