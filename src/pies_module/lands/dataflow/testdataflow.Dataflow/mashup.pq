[DefaultOutputDestinationSettings = 
    [DestinationDefinition = 
        [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], 
     UpdateMethod = [Kind = "Replace"], 
     DestinationTypeSettings = [Kind = "Table"]
    ], 
 StagingDefinition = [Kind = "FastCopy"]
]
section Section1;
//
//-------
//Parameter queries (dynamic via Variable Library)
//-------
//
shared #"pmt-workspace-id" =
    Variable.ValueOrDefault(
        "$(/**/pmt-variable-library/pmt-workspace-id)",
        "79dafa8a-b966-4240-80f7-2e9d46baa5c3"
    )
    meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];

shared #"pmt-lakehouse-id" =
    Variable.ValueOrDefault(
        "$(/**/pmt-variable-library/pmt-lakehouse-id)",
        "201b9b7d-ca11-46b6-81e8-feeba4c99642"
    )
    meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
//
//------
// Main query (bound to default destination)
//------
//
[BindToDefaultDestination = true]
shared test = 
let
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

//------
//Default destination definition
//------
//

[BindToDefaultDestination = true]
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false])
    {[workspaceId = #"pmt-workspace-id"]}[Data]
    {[lakehouseId = #"pmt-lakehouse-id"]}[Data];
