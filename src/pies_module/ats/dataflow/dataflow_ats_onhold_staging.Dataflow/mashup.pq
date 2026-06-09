[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ats_authorizations_on_hold_staging_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ats_authorizations_on_hold_staging = let
    // -----------------------------
    // Parameters
    // -----------------------------
    Source = Lakehouse.Contents(null),
    WS     = Source{[workspaceId = #"pmt-workspace-id"]}[Data],
    LH     = WS{[lakehouseId  = #"pmt-lakehouse-id"]}[Data],

    // -----------------------------
    // Read raw ON_HOLD table
    // -----------------------------
    ONH_Source = LH{[Id = "ats_replication.ats_authorizations_on_hold"]}[Data],

    // -----------------------------
    // Sort first so the kept duplicate is deterministic
    // -----------------------------
    Sorted =
        Table.Sort(
            ONH_Source,
            {
                {"AUTHORIZATION_ID", Order.Ascending},
                {"ON_HOLD_START_DATE", Order.Ascending},
                {"ON_HOLD_END_DATE", Order.Ascending},
                {"ATHN_ON_HOLD_REASON_CODE", Order.Ascending},
                {"AUTHORIZATION_ON_HOLD_ID", Order.Ascending}
            }
        ),

    // -----------------------------
    // Remove duplicates by business key
    // Keep all original columns, only remove duplicate rows
    // -----------------------------
    Deduped =
        Table.Distinct(
            Sorted,
            {
                "AUTHORIZATION_ID",
                "ON_HOLD_START_DATE",
                "ON_HOLD_END_DATE",
                "ATHN_ON_HOLD_REASON_CODE"
            }
        )
in
    Deduped;
shared #"pmt-workspace-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt_env_vars/pmt-workspace-id)",
        "60f10f69-06b8-44a3-a3cf-1afd52d4d754")
in
  Source;
shared #"pmt-lakehouse-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt_env_vars/pmt-lakehouse-id)",
        "964899a6-7e35-47cd-9eb3-429482e1b733")
in
  Source;
shared ats_authorizations_on_hold_staging_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = #"pmt-workspace-id"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = #"pmt-lakehouse-id"]}[Data],
  Navigation_3 = Navigation_2{[Id = "ats_staging", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "ats_authorizations_on_hold_staging", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
