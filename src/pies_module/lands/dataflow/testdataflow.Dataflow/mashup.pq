[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination (2)", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = "79dafa8a-b966-4240-80f7-2e9d46baa5c3" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"pmt-lakehouse-id" = "201b9b7d-ca11-46b6-81e8-feeba4c99642" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
[BindToDefaultDestination = true]
shared test = let
    // -----------------------------
    // Parameters
    // -----------------------------
    WorkspaceId =
        Variable.ValueOrDefault(
            "$(/**/pmt-variable-library/pmt-workspace-id)",
            "79dafa8a-b966-4240-80f7-2e9d46baa5c3"
        ),

    LakehouseId =
        Variable.ValueOrDefault(
            "$(/**/pmt-variable-library/pmt-lakehouse-id)",
            "201b9b7d-ca11-46b6-81e8-feeba4c99642"
        ),
    // Optional: add a constant system_id so rules/preflight won't complain
    DEFAULT_SYSTEM_ID = "ITSM-6072",

    // -----------------------------
    // Lakehouse navigation
    // -----------------------------
    Source = Lakehouse.Contents(null),
    WS = Source{[workspaceId = WorkspaceId]}[Data],
    LH = WS{[lakehouseId = LakehouseId]}[Data],

    // -----------------------------
    // Read source objects
    // -----------------------------
    DISP_Source  = LH{[Id = "tantalis_staging.vw_ta_disposition_merged"]}[Data],
  #"Removed columns" = Table.RemoveColumns(DISP_Source, {"COMPLEXITY_LEVEL_CDE", "CROWN_GRANT_YRN", "DISPOSITION_SID", "DISPOSITION_TRANSACTION_SID", "EXPIRY_DAT", "MANAGING_AGENCY", "ORG_UNIT_SID", "RECEIVED_DAT", "REPLMNT_FOR_DISP_TRANS_SID", "REVIEW_DAT", "SUBTYPE_SID", "TYPE_SID", "UNIT_NAME", "ACTIVATION_CDE", "FILE_CHR", "CODE_CHR_STATUS", "CODE_CHR_STAGE", "EFFECTIVE_DAT", "CODE_CHR_REASON", "TYPE_NME", "SUBTYPE_NME", "PURPOSE_NME", "SUBPURPOSE_NME", "DISTRICT_OFFICE", "LAND_STATUS_DAT", "REPORTED_DAT", "ADJUDICATED_DAT", "DISALLOWED_DAT", "OFFERED_DAT", "OFFER_NOT_ACCEPTED_DAT", "CANCELED_DAT"})
in
    #"Removed columns";
[BindToDefaultDestination = true]
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false])
    {[workspaceId = #"pmt-workspace-id"]}[Data]
    {[lakehouseId = #"pmt-lakehouse-id"]}[Data];
shared #"DefaultDestination (2)" = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data]{[lakehouseId = "201b9b7d-ca11-46b6-81e8-feeba4c99642"]}[Data];
