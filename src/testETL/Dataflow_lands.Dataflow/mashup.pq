[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared ATIS_WATERJOB_COMBINED_MV = let
  Source = Oracle.Database("nrcdb05.bcgov:1521/ewrwtst1.nrs.bcgov", [HierarchicalNavigation = true]),
  #"Navigation 1" = Source{[Schema = "WATERDW"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Name = "ATIS_WATERJOB_COMBINED_MV"]}[Data]
in
  #"Navigation 2";
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data]{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data];
