[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = "60f10f69-06b8-44a3-a3cf-1afd52d4d754" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"pmt-lakehouse-id" = "964899a6-7e35-47cd-9eb3-429482e1b733" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared pies_staging_lands_dispids = let
    // -----------------------------
    // Parameters
    // -----------------------------
    WorkspaceId = Text.From(#"pmt-workspace-id"),
    LakehouseId = Text.From(#"pmt-lakehouse-id"),

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
    // FIX #1: Normalize LEFT-side ATS join key too (ATSAUTHORIZATIONNUMBER)
    // -----------------------------
    ExpandedVFCBC_WithATSKey =
        Table.AddColumn(
            ExpandedVFCBC,
            "__ATSAUTH_KEY",
            each NormalizeKey([ATSAUTHORIZATIONNUMBER]),
            type text
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

    // (Optional) filter/dedup ATS on key to avoid accidental many-to-many
    ATS_Dedup =
        Table.Distinct(
            Table.SelectRows(ATS_WithKey, each [__AUTH_KEY] <> null),
            {"__AUTH_KEY"}
        ),

    JoinedATS =
        Table.NestedJoin(
            ExpandedVFCBC_WithATSKey,
            {"__ATSAUTH_KEY"},
            ATS_Dedup,
            {"__AUTH_KEY"},
            "ATS",
            JoinKind.LeftOuter
        ),

    // -----------------------------
    // FIX #2: Expand the missing ATS columns needed by rules
    // -----------------------------
    ATS_Cols =
        {
            "APPLICATION_ACCEPTED_DATE",
            "APPLICATION_RECEIVED_DATE",
            "APPLICATION_REJECTED_DATE",
            "APPLICATION_FEE",
            "AUTHORIZATION_DUE_DATE",
            "AUTHORIZATION_FEE",
            "ADJUDICATION_DATE",
            "FCBC_PROCESS_COMPLETE_DATE",
            "AUTHORIZATION_STATUS_CODE",
            "ATHN_CLOSE_REASON_CODE",
            "FIRST_NATION_START_DATE",
            "FIRST_NATION_COMPLETION_DATE",
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
    // FIX #3 (optional but recommended): add system_id column for rules/preflight
    // -----------------------------
    WithSystemId =
        Table.AddColumn(
            ExpandedATS,
            "system_id",
            each DEFAULT_SYSTEM_ID,
            type text
        ),

    // -----------------------------
    // PIESID (AFTER ATS JOIN)
    // -----------------------------
    WithPIESID =
        Table.AddColumn(
            WithSystemId,
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
            {"__JOIN_KEY", "__AUTH_KEY", "__ATSAUTH_KEY"},
            MissingField.Ignore
        )
in
    Cleanup;
