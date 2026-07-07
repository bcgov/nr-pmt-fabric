[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "apts_replication apts_ar_permittype_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"apts_replication apts_ar_permittype" = let
  Source = Lakehouse.Contents(null),
  WS     = Source{[workspaceId = #"pmt-workspace-id"]}[Data],
  LH     = WS{[lakehouseId  = #"pmt-lakehouse-id"]}[Data],
  
  // -----------------------------
  // Read source objects
  // -----------------------------
  #"Navigation 2"  = LH{[Id = "apts_replication.apts_ar_permittype"]}[Data]

in
  #"Navigation 2";
shared #"pmt-lakehouse-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt_env_vars/pmt-lakehouse-id)",
        "201b9b7d-ca11-46b6-81e8-feeba4c99642")
in
  Source;
shared #"pmt-workspace-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt_env_vars/pmt-workspace-id)",
        "79dafa8a-b966-4240-80f7-2e9d46baa5c3")
in
  Source;
shared #"apts_replication apts_ar_permittype_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = #"pmt-workspace-id"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId =  #"pmt-lakehouse-id"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "apts_replication apts_ar_permittype", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
