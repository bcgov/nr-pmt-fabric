[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query1_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ats_authorizations_on_hold_staging = let
    // -----------------------------
    // Parameters
    // -----------------------------
    WorkspaceId =
        Variable.ValueOrDefault(
            "$(/**/pmt-variable-library/pmt-workspace-id)",
            "60f10f69-06b8-44a3-a3cf-1afd52d4d754"
        ),

    LakehouseId =
        Variable.ValueOrDefault(
            "$(/**/pmt-variable-library/pmt-lakehouse-id)",
            "964899a6-7e35-47cd-9eb3-429482e1b733"
        ),

    // -----------------------------
    // Lakehouse navigation
    // -----------------------------
    Source = Lakehouse.Contents(null),
    WS = Source{[workspaceId = WorkspaceId]}[Data],
    LH = WS{[lakehouseId = LakehouseId]}[Data],

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
shared #"pmt-workspace-id" = "79dafa8a-b966-4240-80f7-2e9d46baa5c3" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"pmt-lakehouse-id" = "201b9b7d-ca11-46b6-81e8-feeba4c99642" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared Query1_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "201b9b7d-ca11-46b6-81e8-feeba4c99642"]}[Data],
  Navigation_3 = Navigation_2{[Id = "ats_staging", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "ats_authorizations_on_hold_staging", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
