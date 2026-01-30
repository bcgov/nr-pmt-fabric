[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = "79dafa8a-b966-4240-80f7-2e9d46baa5c3" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"pmt-lakehouse-id" = "201b9b7d-ca11-46b6-81e8-feeba4c99642" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
[BindToDefaultDestination = true]
shared pies_staging_lands_dispids = let
    // -----------------------------
    // Parameters
    // -----------------------------
    WorkspaceId = Text.From(#"pmt-workspace-id"),
    LakehouseId = Text.From(#"pmt-lakehouse-id"),

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
    VFCBC_Source = LH{[Id = "tantalis_staging.ta_vfcbc_merged"]}[Data],
    ATS_Source   = LH{[Id = "ats_replication.ats_authorizations"]}[Data],

    // -----------------------------
    // Helper: normalize keys (trim, cast to text, remove trailing ".0", blank -> null)
    // -----------------------------
    NormalizeKey = (v as any) as nullable text =>
        let
            t0 = if v = null then "" else Text.Trim(Text.From(v)),
            t1 = if Text.EndsWith(t0, ".0") then Text.BeforeDelimiter(t0, ".") else t0,
            t2 = Text.Trim(t1)
        in
            if t2 = "" then null else t2,

    // -----------------------------
    // Expand helper: DO NOT rename unless there is a name conflict
    // If a column already exists in the outer table, only then it gets prefixed.
    // -----------------------------
    ExpandUnlessConflict =
        (outer as table, nestedCol as text, cols as list, conflictPrefix as text) as table =>
            let
                existing = Table.ColumnNames(outer),
                newNames = List.Transform(cols, (c) => if List.Contains(existing, c) then conflictPrefix & c else c)
            in
                Table.ExpandTableColumn(outer, nestedCol, cols, newNames),

    // -----------------------------
    // DISP key = DISPOSITION_TRANSACTION_SID
    // -----------------------------
    DISP_WithKey =
        Table.AddColumn(
            DISP_Source,
            "__JOIN_KEY",
            each NormalizeKey([DISPOSITION_TRANSACTION_SID]),
            type text
        ),

    // -----------------------------
    // VFCBC key = BUSINESSAREANUMBER
    // -----------------------------
    VFCBC_WithKey =
        Table.AddColumn(
            VFCBC_Source,
            "__JOIN_KEY",
            each NormalizeKey([BUSINESSAREANUMBER]),
            type text
        ),

    // DISTINCT VFCBC on join key
    VFCBC_Dedup =
        Table.Distinct(
            Table.SelectRows(VFCBC_WithKey, each [__JOIN_KEY] <> null),
            {"__JOIN_KEY"}
        ),

    // -----------------------------
    // LEFT JOIN (driver = DISP)
    // -----------------------------
    Joined =
        Table.NestedJoin(
            DISP_WithKey,
            {"__JOIN_KEY"},
            VFCBC_Dedup,
            {"__JOIN_KEY"},
            "VFCBC",
            JoinKind.LeftOuter
        ),

    // -----------------------------
    // Expand VFCBC columns (keep original names unless conflict)
    // -----------------------------
    VFCBC_Cols =
        {
            "JOBID","TRACKINGNUMBER","BUSINESSAREANUMBER_CORAL","ATSLINKID","LANDSFILENUMBER",
            "SUBMITTER","APPLICANT","AUTHORIZATIONTYPE","APPLICATIONTYPE","CREATEDDATE","STATUS",
            "OFFICE","BUSINESSAREA","SUBMITTEDDATE","RECEIVEDDATE","DATECOMPLETED",
            "ATSAUTHORIZATIONNUMBER","ATS_LANDSFILENUMBER","BUSINESSAREANUMBER"
        },

    ExpandedVFCBC =
        ExpandUnlessConflict(
            Joined,
            "VFCBC",
            VFCBC_Cols,
            "VFCBC_"
        ),

    // -----------------------------
    // ATS LOGIC (join via ATSAUTHORIZATIONNUMBER -> ATS.AUTHORIZATION_ID)
    // -----------------------------
    ATS_WithKey =
        Table.AddColumn(
            ATS_Source,
            "__AUTH_KEY",
            each NormalizeKey([AUTHORIZATION_ID]),
            type text
        ),

    JoinedATS =
        Table.NestedJoin(
            ExpandedVFCBC,
            {"ATSAUTHORIZATIONNUMBER"},
            ATS_WithKey,
            {"__AUTH_KEY"},
            "ATS",
            JoinKind.LeftOuter
        ),

    // Expand ATS columns (keep original names unless conflict)
    ATS_Cols =
        {
            "APPLICATION_ACCEPTED_DATE",
            "APPLICATION_RECEIVED_DATE",
            "APPLICATION_FEE",
            "AUTHORIZATION_DUE_DATE",
            "AUTHORIZATION_FEE",
            "ADJUDICATION_DATE",
            "FCBC_PROCESS_COMPLETE_DATE",
            "AUTHORIZATION_STATUS_CODE",
            "FILE_NUMBER",
            "WHEN_CREATED",
            "WHEN_UPDATED"
        },

    ExpandedATS =
        ExpandUnlessConflict(
            JoinedATS,
            "ATS",
            ATS_Cols,
            "ATS_"
        ),

    // -----------------------------
    // PIESID (AFTER ATS JOIN)
    // -----------------------------
    WithPIESID =
        Table.AddColumn(
            ExpandedATS,
            "PIESID",
            each NormalizeKey([DISPOSITION_TRANSACTION_SID]),
            type text
        ),

    // Remove null/empty PIESID rows
    FilterPIESID =
        Table.SelectRows(WithPIESID, each [PIESID] <> null),

    // -----------------------------
    // Cleanup helper columns
    // -----------------------------
    Cleanup =
        Table.RemoveColumns(
            FilterPIESID,
            {"__JOIN_KEY", "__AUTH_KEY"},
            MissingField.Ignore
        )
in
    Cleanup;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "60f10f69-06b8-44a3-a3cf-1afd52d4d754"]}[Data]{[lakehouseId = "964899a6-7e35-47cd-9eb3-429482e1b733"]}[Data];
