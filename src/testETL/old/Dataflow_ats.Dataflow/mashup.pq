[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ats_replication ats_authorizations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"ats_replication ats_authorizations" = let
  Source = PostgreSQL.Database("theory.bcgov:5434", "odstst"),
  #"Navigation 1" = Source{[Schema = "ats_replication", Item = "ats_authorizations"]}[Data]
in
  #"Navigation 1";
shared #"ats_replication ats_authorizations_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data],
  TableNavigation = Navigation_2{[Id = "ats_replication ats_authorizations", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data]{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data];
