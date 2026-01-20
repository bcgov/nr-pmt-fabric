[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared pmt_lands_dataflow_authorizationid_piesid = let
    // ---------- 1. Load Lakehouse data ----------
    Source = Lakehouse.Contents(null),
    Workspace = Source{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],

    // Lakehouse with ATS + VFCBC views
    LakehouseATS = Workspace{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data],
    ats_authorizations = LakehouseATS{[Id = "ats_replication ats_authorizations", ItemKind = "Table"]}[Data],
    VFCBC_APPLICATION_VW = LakehouseATS{[Id = "VFCBC_APPLICATION_VW", ItemKind = "Table"]}[Data],
    VFCBC_ATSNUMBER_VW = LakehouseATS{[Id = "VFCBC_ATSNUMBER_VW", ItemKind = "Table"]}[Data],

    // Lakehouse with TANTALIS staging
    LakehouseTantalis = Workspace{[lakehouseId = "201b9b7d-ca11-46b6-81e8-feeba4c99642"]}[Data],
    ta_disposition_merged = LakehouseTantalis{[Id = "tantalis_staging.ta_disposition_merged", ItemKind = "Table"]}[Data],

    // ---------- 2. Build VFCBC_joined ----------
    // VFCBC_APPLICATION_VW LEFT JOIN VFCBC_ATSNUMBER_VW ON ATSLINKID
    VFC_Merged = Table.NestedJoin(
        VFCBC_APPLICATION_VW,
        {"ATSLINKID"},
        VFCBC_ATSNUMBER_VW,
        {"ATSLINKID"},
        "Ats",
        JoinKind.LeftOuter
    ),
    // Expand selected columns from ATSNUMBER and rename ATSAUTHORIZATIONNUMBER -> authorization_id
    VFC_Expanded = Table.ExpandTableColumn(
        VFC_Merged,
        "Ats",
        {"ATSAUTHORIZATIONNUMBER", "DISPOSITION_TRANSACTION_SID", "DISPOSITION_TRANSACTION_STRING"},
        {"authorization_id", "VFC_DISPOSITION_TRANSACTION_SID", "DISPOSITION_TRANSACTION_STRING"}
    ),

    // ---------- 3. Build ta_disp_auth ---------- 
    // Add authorization_id column from DISPOSITION_TRANSACTION_SID in TANTALIS
    Disp_WithAuthId = Table.AddColumn(
        ta_disposition_merged,
        "authorization_id",
        each [DISPOSITION_TRANSACTION_SID]
    ),
    // LEFT JOIN ats_authorizations USING authorization_id
    Disp_Merged = Table.NestedJoin(
        Disp_WithAuthId,
        {"authorization_id"},
        ats_authorizations,
        {"AUTHORIZATION_ID"},
        "Auth",
        JoinKind.LeftOuter
    ),
    // Expand a few useful columns from ats_authorizations (you can add more if needed)
    Disp_Expanded = Table.ExpandTableColumn(
        Disp_Merged,
        "Auth",
        {"authorization_id", "file_number"},
        {"ATS_authorization_id", "ATS_file_number"}
    ),

    // ---------- 4. Final join: ta_disp_auth LEFT JOIN VFCBC_joined USING authorization_id ----------
    Final_Merged = Table.NestedJoin(
        Disp_Expanded,
        {"authorization_id"},
        VFC_Expanded,
        {"authorization_id"},
        "VFC",
        JoinKind.LeftOuter
    ),
    // Expand VFCBC_joined columns (skip its authorization_id to avoid conflict)
    Final_Expanded = Table.ExpandTableColumn(
        Final_Merged,
        "VFC",
        {
            "JOBID",
            "BUSINESSAREANUMBER",
            "ATSLINKID",
            "ATSPROJECTNUMBER",
            "EXTERNALREFERENCE",
            "EXTERNALJOBNUMBER",
            "EXTERNALJOBNUMBER2",
            "MINENUMBER",
            "LANDSFILENUMBER",
            "AUTHORIZATIONTYPE",
            "APPLICATIONTYPE",
            "STATUS",
            "OFFICE",
            "BUSINESSAREA",
            "SUBMITTEDBYTYPE",
            "STATUSSHOWNTOCLIENT",
            "SUBMITTEDDATE",
            "DATECOMPLETED",
            "VFCTRACKING_NUMBER",
            "VFC_DISPOSITION_TRANSACTION_SID",
            "DISPOSITION_TRANSACTION_STRING"
        },
        {
            "JOBID",
            "BUSINESSAREANUMBER",
            "ATSLINKID",
            "ATSPROJECTNUMBER",
            "EXTERNALREFERENCE",
            "EXTERNALJOBNUMBER",
            "EXTERNALJOBNUMBER2",
            "MINENUMBER",
            "VFC_LANDSFILENUMBER",
            "AUTHORIZATIONTYPE",
            "APPLICATIONTYPE",
            "STATUS",
            "OFFICE",
            "BUSINESSAREA",
            "SUBMITTEDBYTYPE",
            "STATUSSHOWNTOCLIENT",
            "SUBMITTEDDATE",
            "DATECOMPLETED",
            "VFCTRACKING_NUMBER",
            "VFC_DISPOSITION_TRANSACTION_SID",
            "DISPOSITION_TRANSACTION_STRING"
        }
    ),

    // ---------- 5. Final rename: authorization_id -> pies_id ----------
    Final_Renamed = Table.RenameColumns(
        Final_Expanded,
        {{"authorization_id", "pies_id"}}
    )
in
    Final_Renamed;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data]{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data];
