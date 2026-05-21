[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt-variable-library/pmt-workspace-id)",
        "79dafa8a-b966-4240-80f7-2e9d46baa5c3")
in
  Source;
shared #"pmt-lakehouse-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt-variable-library/pmt-lakehouse-id)",
        "201b9b7d-ca11-46b6-81e8-feeba4c99642")
in
  Source;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "test_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared test = let
    Source = Lakehouse.Contents(null),
    WS     = Source{[workspaceId = #"pmt-workspace-id"]}[Data],
    LH     = WS{[lakehouseId  = #"pmt-lakehouse-id"]}[Data],

    // -----------------------------
    // Read source objects
    // -----------------------------
    DISP_Source  = LH{[Id = "tantalis_staging.vw_ta_disposition_merged"]}[Data],
  #"Removed columns" = Table.RemoveColumns(DISP_Source, {"COMPLEXITY_LEVEL_CDE", "CROWN_GRANT_YRN", "DISPOSITION_SID", "DISPOSITION_TRANSACTION_SID", "EXPIRY_DAT", "MANAGING_AGENCY", "ORG_UNIT_SID", "RECEIVED_DAT", "REPLMNT_FOR_DISP_TRANS_SID", "REVIEW_DAT", "SUBTYPE_SID", "TYPE_SID", "UNIT_NAME", "ACTIVATION_CDE", "FILE_CHR", "CODE_CHR_STATUS", "CODE_CHR_STAGE", "EFFECTIVE_DAT", "CODE_CHR_REASON", "TYPE_NME", "SUBTYPE_NME", "PURPOSE_NME", "SUBPURPOSE_NME", "DISTRICT_OFFICE", "LAND_STATUS_DAT", "REPORTED_DAT", "ADJUDICATED_DAT", "DISALLOWED_DAT", "OFFERED_DAT", "OFFER_NOT_ACCEPTED_DAT", "CANCELED_DAT"})
in
    #"Removed columns";
shared test_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = #"pmt-workspace-id"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = #"pmt-lakehouse-id"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "test", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
