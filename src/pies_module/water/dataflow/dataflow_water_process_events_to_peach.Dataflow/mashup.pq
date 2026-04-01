[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination (2)", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = "60f10f69-06b8-44a3-a3cf-1afd52d4d754" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared #"pmt-lakehouse-id" = "964899a6-7e35-47cd-9eb3-429482e1b733" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
[BindToDefaultDestination = true]
shared pies_staging_water_jobnumber = let
    // ============================================================
    // Parameters
    // ============================================================

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

    // ============================================================
    // Lakehouse navigation
    // ============================================================
    Source = Lakehouse.Contents(null),
    WS = Source{[workspaceId = WorkspaceId]}[Data],
    LH = WS{[lakehouseId = LakehouseId]}[Data],

    GetLHById = (id as text, friendly as text) as table =>
        let
            rows = Table.SelectRows(LH, each Record.HasFields(_, "Id") and [Id] = id),
            ranked =
                if Table.RowCount(rows) = 0 then
                    rows
                else
                    Table.AddColumn(
                        rows,
                        "__rank",
                        each if [ItemKind] = "Table" then 1 else if [ItemKind] = "View" then 2 else 9,
                        Int64.Type
                    ),
            best =
                if Table.RowCount(ranked) = 0 then
                    error ("Lakehouse object not found: " & friendly & " (Id=" & id & ")")
                else
                    Table.Sort(ranked, {{"__rank", Order.Ascending}}){0}[Data]
        in
            best,

    ATIS0       = GetLHById("wma_replication.atis_waterjob_combined_mv", "ATIS_WATERJOB_COMBINED_MV"),
    VFCBC_APP0  = GetLHById("vfcbc_replication.vfcbc_application_vw", "VFCBC_APPLICATION_VW"),
    VFCBC_ATS0  = GetLHById("vfcbc_replication.vfcbc_atsnumber_vw", "VFCBC_ATSNUMBER_VW"),
    ATS_AUTH0   = GetLHById("ats_replication.ats_authorizations", "ATS_AUTHORIZATIONS"),
    ONH0        = GetLHById("ats_replication.ats_authorizations_on_hold", "ATS_AUTHORIZATIONS_ON_HOLD"),

    // ============================================================
    // Helpers
    // ============================================================
    NormalizeCols = (t as table) as table =>
        Table.TransformColumnNames(t, each Text.Upper(Text.Trim(_))),

    IsNullOrBlank = (v as any) as logical =>
        v = null or Text.Trim(Text.From(v)) = "",

    EnsureCanonicalFromAliases = (t as table, canonical as text, aliases as list, tname as text) as table =>
        let
            cols = Table.ColumnNames(t),
            hit =
                if List.Contains(cols, canonical) then
                    canonical
                else
                    let found = List.Select(aliases, each List.Contains(cols, _))
                    in if List.Count(found) > 0 then found{0} else null
        in
            if hit = null then
                error (tname & " missing required column '" & canonical & "'. Available columns: " & Text.Combine(cols, ", "))
            else if hit = canonical then
                t
            else
                Table.AddColumn(t, canonical, each Record.Field(_, hit), type any),

    // Safe “time-only” detector
    IsTimeOnlyText = (s as text) as logical =>
        let
            t = Text.Trim(s),
            hasColon = Text.Contains(t, ":"),
            hasDash  = Text.Contains(t, "-"),
            hasSlash = Text.Contains(t, "/"),
            hasT     = Text.Contains(t, "T"),
            hasComma = Text.Contains(t, ",")
        in
            hasColon and (not hasDash) and (not hasSlash) and (not hasT) and (not hasComma),

    CleanDateValue = (v as any) as any =>
        if v = null then
            null
        else if Value.Is(v, type time) or Value.Is(v, type duration) then
            null
        else if Value.Is(v, type text) and IsTimeOnlyText(v) then
            null
        else
            v,

    FirstUsefulField = (r as record, names as list) as any =>
        let
            vals = List.Transform(
                names,
                (n) =>
                    let raw = try Record.Field(r, n) otherwise null,
                        v = CleanDateValue(raw)
                    in
                        if v = null then null
                        else if Value.Is(v, type text) and Text.Trim(v) = "" then null
                        else v
            ),
            nn = List.RemoveNulls(vals)
        in
            if List.Count(nn) > 0 then nn{0} else null,

    ToKeyText = (v as any) as nullable text =>
        if v = null then null
        else
            let t = Text.Trim(Text.From(v))
            in if t = "" then null else t,

    ToNumberOrNull = (v as any) as nullable number =>
        try Number.From(v) otherwise null,


    ExpandWithDedup = (left as table, nestedCol as text, rightCols as list, suffix as text) as table =>
        let
            existing = Table.ColumnNames(left),
            newNames = List.Transform(rightCols, each if List.Contains(existing, _) then _ & suffix else _),
            expanded = Table.ExpandTableColumn(left, nestedCol, rightCols, newNames)
        in
            expanded,

    // ============================================================
    // Normalize column names
    // ============================================================
    ATIS1      = NormalizeCols(ATIS0),
    APP1       = NormalizeCols(VFCBC_APP0),
    ATSNO1     = NormalizeCols(VFCBC_ATS0),
    AUTH1      = NormalizeCols(ATS_AUTH0),
    ONH1       = NormalizeCols(ONH0),

    // ============================================================
    // Ensure join columns exist
    // ============================================================
    ATIS2 =
        EnsureCanonicalFromAliases(
            ATIS1,
            "VFCTRACKING_NUMBER",
            {"VFCTRACKING_NUMBER", "VFCTRACKINGNUMBER", "VFC_TRACKING_NUMBER", "VFC_TRACKINGNUMBER", "TRACKINGNUMBER"},
            "ATIS"
        ),

    APP2 =
        EnsureCanonicalFromAliases(
            APP1,
            "VFCTRACKING_NUMBER",
            {"VFCTRACKING_NUMBER", "VFCTRACKINGNUMBER", "VFC_TRACKING_NUMBER", "VFC_TRACKINGNUMBER", "TRACKINGNUMBER"},
            "VFCBC_APPLICATION_VW"
        ),


    APP3 =
        EnsureCanonicalFromAliases(
            APP2,
            "ATSLINKID",
            {"ATSLINKID", "ATS_LINK_ID", "ATS_LINKID"},
            "VFCBC_APPLICATION_VW"
        ),

    ATSNO2 =
        EnsureCanonicalFromAliases(
            ATSNO1,
            "ATSLINKID",
            {"ATSLINKID", "ATS_LINK_ID", "ATS_LINKID"},
            "VFCBC_ATSNUMBER_VW"
        ),

    ATSNO3 =
        EnsureCanonicalFromAliases(
            ATSNO2,
            "ATSAUTHORIZATIONNUMBER",
            {"ATSAUTHORIZATIONNUMBER", "ATS_AUTHORIZATION_NUMBER", "ATSAUTHNUMBER"},
            "VFCBC_ATSNUMBER_VW"
        ),


    AUTH2 =
        EnsureCanonicalFromAliases(
            AUTH1,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATS_AUTHORIZATIONS"
        ),

    ONH2 =
        EnsureCanonicalFromAliases(
            ONH1,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATS_AUTHORIZATIONS_ON_HOLD"
        ),


    ATIS3 =
        EnsureCanonicalFromAliases(
            ATIS2,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATIS"
        ),


    ATIS4 = Table.AddColumn(ATIS3, "__VFCT_KEY", each ToKeyText([VFCTRACKING_NUMBER]), type text),
    APP4  = Table.AddColumn(APP3,  "__VFCT_KEY", each ToKeyText([VFCTRACKING_NUMBER]), type text),

    APP5  = Table.AddColumn(APP4,  "__ATSLINK_KEY", each ToKeyText([ATSLINKID]), type text),
    ATSNO4= Table.AddColumn(ATSNO3,"__ATSLINK_KEY", each ToKeyText([ATSLINKID]), type text),



    J0 =
        Table.NestedJoin(
            ATIS4,
            {"__VFCT_KEY"},
            APP5,
            {"__VFCT_KEY"},
            "APP",
            JoinKind.LeftOuter
        ),


    AppCols = Table.ColumnNames(APP5),
    E0 = ExpandWithDedup(J0, "APP", AppCols, "__APP"),

    // For ATSNO join
    WithATSLINKID =
        Table.AddColumn(
            E0,
            "__ATSLINK_FOR_JOIN",
            each
                let
                    v = FirstUsefulField(_, {"ATSLINKID", "ATSLINKID__APP"})
                in
                    ToKeyText(v),
            type text
        ),

    J1 =
        Table.NestedJoin(
            WithATSLINKID,
            {"__ATSLINK_FOR_JOIN"},
            ATSNO4,
            {"__ATSLINK_KEY"},
            "ATSNO",
            JoinKind.LeftOuter
        ),

    AtsNoCols = Table.ColumnNames(ATSNO4),
    E1 = ExpandWithDedup(J1, "ATSNO", AtsNoCols, "__ATSNO"),


    WithAuthKey =
        Table.AddColumn(
            E1,
            "__AUTH_ID_FOR_JOIN",
            each
                let
                    aRaw = FirstUsefulField(_, {"AUTHORIZATION_ID"}),
                    a = ToNumberOrNull(aRaw),
                    bRaw = FirstUsefulField(_, {"ATSAUTHORIZATIONNUMBER", "ATSAUTHORIZATIONNUMBER__ATSNO"}),
                    b = ToNumberOrNull(bRaw)
                in
                    if a <> null then a else b,
            type number
        ),

    AUTH3 = Table.TransformColumns(AUTH2, {{"AUTHORIZATION_ID", each ToNumberOrNull(_), type number}}),

    J2 =
        Table.NestedJoin(
            WithAuthKey,
            {"__AUTH_ID_FOR_JOIN"},
            AUTH3,
            {"AUTHORIZATION_ID"},
            "AUTH",
            JoinKind.LeftOuter
        ),

    AuthCols = Table.ColumnNames(AUTH3),
    E2 = ExpandWithDedup(J2, "AUTH", AuthCols, "__AUTH"),

    ONH3 = Table.TransformColumns(ONH2, {{"AUTHORIZATION_ID", each ToNumberOrNull(_), type number}}),

    J3 =
        Table.NestedJoin(
            E2,
            {"__AUTH_ID_FOR_JOIN"},
            ONH3,
            {"AUTHORIZATION_ID"},
            "ONH",
            JoinKind.LeftOuter
        ),

    OnhCols =
        {
            "AUTHORIZATION_ON_HOLD_ID",
            "ATHN_ON_HOLD_REASON_CODE",
            "CURRENT_ON_HOLD_IND",
            "ON_HOLD_START_DATE",
            "ON_HOLD_END_DATE",
            "ON_HOLD_NOTES",
            "REMOVED_BY",
            "PARTNER_AGENCY_ID"
        },

    E3 = ExpandWithDedup(J3, "ONH", OnhCols, "__ONH"),


    WithPIESID =
        Table.AddColumn(
            E3,
            "PIESID",
            each
                if not IsNullOrBlank([JOBNUMBER])
                then Text.From([JOBNUMBER])
                else null,
            type text
        ),


    EnsureIfMissing = (t as table, col as text, compute as function, colType as type) as table =>
        if List.Contains(Table.ColumnNames(t), col) then t else Table.AddColumn(t, col, compute, colType),


    S0  = EnsureIfMissing(WithPIESID, "CREATEDDATE", each FirstUsefulField(_, {"CREATEDDATE"}), type any),
    S1  = EnsureIfMissing(S0,        "SUBMITTEDDATE", each FirstUsefulField(_, {"SUBMITTEDDATE"}), type any),

    S2  = EnsureIfMissing(S1, "APPLICATION_ACCEPTED_DATE",
            each FirstUsefulField(_, {"APPLICATION_ACCEPTED_DATE", "FCBCACCEPTEDDATE", "RECEIVEDDATE"}),
            type any),

    S3  = EnsureIfMissing(S2, "APPLICATION_RECEIVED_DATE",
            each FirstUsefulField(_, {"APPLICATION_RECEIVED_DATE", "RECEIVEDDATE"}),
            type any),

    S4  = EnsureIfMissing(S3, "APPLICATION_REJECTED_DATE",
            each FirstUsefulField(_, {"APPLICATION_REJECTED_DATE"}),
            type any),

    S5  = EnsureIfMissing(S4, "AUTHORIZATION_STATUS_CODE",
            each FirstUsefulField(_, {"AUTHORIZATION_STATUS_CODE"}),
            type any),

    S6  = EnsureIfMissing(S5, "ATHN_CLOSE_REASON_CODE",
            each FirstUsefulField(_, {"ATHN_CLOSE_REASON_CODE"}),
            type any),

    S7  = EnsureIfMissing(S6, "ADJUDICATION_DATE",
            each FirstUsefulField(_, {"ADJUDICATION_DATE", "GRANTEDREFUSEDATE", "ATSDECISIONDATE"}),
            type any),

    S8  = EnsureIfMissing(S7, "FIRST_NATION_START_DATE",
            each FirstUsefulField(_, {"FIRST_NATION_START_DATE"}),
            type any),

    S9  = EnsureIfMissing(S8, "FIRST_NATION_COMPLETION_DATE",
            each FirstUsefulField(_, {"FIRST_NATION_COMPLETION_DATE"}),
            type any),


    S10 = EnsureIfMissing(S9, "ADJUDICATED_DAT",
            each FirstUsefulField(_, {"ADJUDICATED_DAT", "ADJUDICATION_DATE"}),
            type any),


    S11 = EnsureIfMissing(S10, "LAND_STATUS_DAT",
            each FirstUsefulField(_, {"LAND_STATUS_DAT", "TECH_REVIEW_COMPLETION_DATE", "TECHASSESSMENTCOMPLETEDATE"}),
            type any),

    S12 = EnsureIfMissing(S11, "REPORTED_DAT",
            each FirstUsefulField(_, {"REPORTED_DAT", "FCBC_PROCESS_COMPLETE_DATE", "FCBCCOMPLETEDATE", "GRANTEDREFUSEDATE"}),
            type any),

    S13 = EnsureIfMissing(S12, "DISALLOWED_DAT",
            each FirstUsefulField(_, {"DISALLOWED_DAT", "APPLICATION_REJECTED_DATE"}),
            type any),

    S14 = EnsureIfMissing(S13, "OFFERED_DAT", each FirstUsefulField(_, {"OFFERED_DAT"}), type any),
    S15 = EnsureIfMissing(S14, "OFFER_NOT_ACCEPTED_DAT", each FirstUsefulField(_, {"OFFER_NOT_ACCEPTED_DAT"}), type any),
    S16 = EnsureIfMissing(S15, "COMMENCEMENT_DAT", each FirstUsefulField(_, {"COMMENCEMENT_DAT"}), type any),
    S17 = EnsureIfMissing(S16, "CANCELED_DAT", each FirstUsefulField(_, {"CANCELED_DAT"}), type any),
    S18 = EnsureIfMissing(S17, "EXPIRY_DAT", each FirstUsefulField(_, {"EXPIRY_DAT", "AUTHORIZATION_DUE_DATE"}), type any),

    S19 = EnsureIfMissing(S18, "CODE_CHR_STATUS", each null, type any),

    S20 = EnsureIfMissing(
            S19,
            "CODE_CHR_STATUS != 'CA'",
            each
                let v = FirstUsefulField(_, {"CODE_CHR_STATUS"})
                in if v = null or IsNullOrBlank(v) then "OK" else Text.From(v),
            type text
         ),

    S21 = EnsureIfMissing(
            S20,
            "CODE_CHR_STATUS != 'DI'",
            each
                let v = FirstUsefulField(_, {"CODE_CHR_STATUS"})
                in if v = null or IsNullOrBlank(v) then "OK" else Text.From(v),
            type text
         ),


    Cleaned =
        Table.RemoveColumns(
            S21,
            List.Intersect({Table.ColumnNames(S21), {"__VFCT_KEY", "__ATSLINK_KEY", "__ATSLINK_FOR_JOIN", "__AUTH_ID_FOR_JOIN"}})
        ),

    Output =
        Table.SelectRows(Cleaned, each [PIESID] <> null and Text.Trim(Text.From([PIESID])) <> "")
in
    Output;
[BindToDefaultDestination = true]
shared pies_staging_water_vfcbctracking = let
    // ============================================================
    // Parameters (keep for CI/CD)
    // ============================================================
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
    // ============================================================
    // Lakehouse navigation
    // ============================================================
    Source = Lakehouse.Contents(null),
    WS = Source{[workspaceId = WorkspaceId]}[Data],
    LH = WS{[lakehouseId = LakehouseId]}[Data],

    GetLHById = (id as text, friendly as text) as table =>
        let
            rows = Table.SelectRows(LH, each Record.HasFields(_, "Id") and [Id] = id),
            ranked =
                if Table.RowCount(rows) = 0 then
                    rows
                else
                    Table.AddColumn(
                        rows,
                        "__rank",
                        each if [ItemKind] = "Table" then 1 else if [ItemKind] = "View" then 2 else 9,
                        Int64.Type
                    ),
            best =
                if Table.RowCount(ranked) = 0 then
                    error ("Lakehouse object not found: " & friendly & " (Id=" & id & ")")
                else
                    Table.Sort(ranked, {{"__rank", Order.Ascending}}){0}[Data]
        in
            best,

    ATIS0       = GetLHById("wma_replication.atis_waterjob_combined_mv", "ATIS_WATERJOB_COMBINED_MV"),
    VFCBC_APP0  = GetLHById("vfcbc_replication.vfcbc_application_vw", "VFCBC_APPLICATION_VW"),
    VFCBC_ATS0  = GetLHById("vfcbc_replication.vfcbc_atsnumber_vw", "VFCBC_ATSNUMBER_VW"),
    ATS_AUTH0   = GetLHById("ats_replication.ats_authorizations", "ATS_AUTHORIZATIONS"),

    // ============================================================
    // Helpers
    // ============================================================
    NormalizeCols = (t as table) as table =>
        Table.TransformColumnNames(t, each Text.Upper(Text.Trim(_))),

    IsNullOrBlank = (v as any) as logical =>
        v = null or Text.Trim(Text.From(v)) = "",

    EnsureCanonicalFromAliases = (t as table, canonical as text, aliases as list, tname as text) as table =>
        let
            cols = Table.ColumnNames(t),
            hit =
                if List.Contains(cols, canonical) then
                    canonical
                else
                    let found = List.Select(aliases, each List.Contains(cols, _))
                    in if List.Count(found) > 0 then found{0} else null
        in
            if hit = null then
                error (tname & " missing required column '" & canonical & "'. Available columns: " & Text.Combine(cols, ", "))
            else if hit = canonical then
                t
            else
                Table.AddColumn(t, canonical, each Record.Field(_, hit), type any),

    IsTimeOnlyText = (s as text) as logical =>
        let
            t = Text.Trim(s),
            hasColon = Text.Contains(t, ":"),
            hasDash  = Text.Contains(t, "-"),
            hasSlash = Text.Contains(t, "/"),
            hasT     = Text.Contains(t, "T"),
            hasComma = Text.Contains(t, ",")
        in
            hasColon and (not hasDash) and (not hasSlash) and (not hasT) and (not hasComma),

    CleanDateValue = (v as any) as any =>
        if v = null then
            null
        else if Value.Is(v, type time) or Value.Is(v, type duration) then
            null
        else if Value.Is(v, type text) and IsTimeOnlyText(v) then
            null
        else
            v,

    FirstUsefulField = (r as record, names as list) as any =>
        let
            vals = List.Transform(
                names,
                (n) =>
                    let
                        raw = try Record.Field(r, n) otherwise null,
                        v = CleanDateValue(raw)
                    in
                        if v = null then null
                        else if Value.Is(v, type text) and Text.Trim(v) = "" then null
                        else v
            ),
            nn = List.RemoveNulls(vals)
        in
            if List.Count(nn) > 0 then nn{0} else null,

    ToStableText = (v as any) as nullable text =>
        if v = null then
            null
        else
            let
                raw =
                    if Value.Is(v, type number) then
                        Number.ToText(v, "0.############################", "en-US")
                    else
                        Text.From(v),
                noNbsp = Text.Replace(raw, Character.FromNumber(160), " "),
                trimmed = Text.Trim(noNbsp)
            in
                if trimmed = "" then null else trimmed,

    ToKeyText = (v as any) as nullable text =>
        ToStableText(v),

    ToNumberOrNull = (v as any) as nullable number =>
        let
            t = ToStableText(v)
        in
            if t = null then
                null
            else
                let
                    parsed =
                        try Number.FromText(t, "en-US")
                        otherwise
                            try Number.From(t)
                            otherwise null
                in
                    parsed,

    ToDateTimeOrNull = (v as any) as nullable datetime =>
        let
            c = CleanDateValue(v)
        in
            if c = null then
                null
            else
                try DateTime.From(c)
                otherwise
                    try DateTime.FromText(ToStableText(c), "en-US")
                    otherwise null,

    ExpandWithDedup = (left as table, nestedCol as text, rightCols as list, suffix as text) as table =>
        let
            existing = Table.ColumnNames(left),
            newNames = List.Transform(rightCols, each if List.Contains(existing, _) then _ & suffix else _),
            expanded = Table.ExpandTableColumn(left, nestedCol, rightCols, newNames)
        in
            expanded,

    EnsureIfMissing = (t as table, col as text, compute as function, colType as type) as table =>
        if List.Contains(Table.ColumnNames(t), col) then t else Table.AddColumn(t, col, compute, colType),

    // ============================================================
    // Normalize column names
    // ============================================================
    ATIS1      = NormalizeCols(ATIS0),
    APP1       = NormalizeCols(VFCBC_APP0),
    ATSNO1     = NormalizeCols(VFCBC_ATS0),
    AUTH1      = NormalizeCols(ATS_AUTH0),

    // ============================================================
    // Ensure join columns exist
    // ============================================================
    ATIS2 =
        EnsureCanonicalFromAliases(
            ATIS1,
            "VFCTRACKING_NUMBER",
            {"VFCTRACKING_NUMBER", "VFCTRACKINGNUMBER", "VFC_TRACKING_NUMBER", "VFC_TRACKINGNUMBER", "TRACKINGNUMBER"},
            "ATIS"
        ),

    APP2 =
        EnsureCanonicalFromAliases(
            APP1,
            "VFCTRACKING_NUMBER",
            {"VFCTRACKING_NUMBER", "VFCTRACKINGNUMBER", "VFC_TRACKING_NUMBER", "VFC_TRACKINGNUMBER", "TRACKINGNUMBER"},
            "VFCBC_APPLICATION_VW"
        ),

    APP3 =
        EnsureCanonicalFromAliases(
            APP2,
            "ATSLINKID",
            {"ATSLINKID", "ATS_LINK_ID", "ATS_LINKID"},
            "VFCBC_APPLICATION_VW"
        ),

    ATSNO2 =
        EnsureCanonicalFromAliases(
            ATSNO1,
            "ATSLINKID",
            {"ATSLINKID", "ATS_LINK_ID", "ATS_LINKID"},
            "VFCBC_ATSNUMBER_VW"
        ),

    ATSNO3 =
        EnsureCanonicalFromAliases(
            ATSNO2,
            "ATSAUTHORIZATIONNUMBER",
            {"ATSAUTHORIZATIONNUMBER", "ATS_AUTHORIZATION_NUMBER", "ATSAUTHNUMBER"},
            "VFCBC_ATSNUMBER_VW"
        ),

    AUTH2 =
        EnsureCanonicalFromAliases(
            AUTH1,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATS_AUTHORIZATIONS"
        ),


    ATIS3 =
        EnsureCanonicalFromAliases(
            ATIS2,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATIS"
        ),

    // ============================================================
    // Build join keys
    // ============================================================
    ATIS4 =
        Table.AddColumn(
            ATIS3,
            "__VFCT_KEY",
            each ToKeyText([VFCTRACKING_NUMBER]),
            type text
        ),

    APP4 =
        Table.AddColumn(
            APP3,
            "__VFCT_KEY",
            each ToKeyText([VFCTRACKING_NUMBER]),
            type text
        ),

    APP5 =
        Table.AddColumn(
            APP4,
            "__ATSLINK_KEY",
            each ToNumberOrNull([ATSLINKID]),
            type number
        ),

    ATSNO4 =
        Table.AddColumn(
            ATSNO3,
            "__ATSLINK_KEY",
            each ToNumberOrNull([ATSLINKID]),
            type number
        ),


    APP5_Distinct   = Table.Distinct(APP5),
    ATSNO4_Distinct = Table.Distinct(ATSNO4),

    // ============================================================
    // Join 1: ATIS -> APP
    // ============================================================
    J0 =
        Table.NestedJoin(
            ATIS4,
            {"__VFCT_KEY"},
            APP5_Distinct,
            {"__VFCT_KEY"},
            "APP",
            JoinKind.LeftOuter
        ),

    AppCols = Table.ColumnNames(APP5_Distinct),
    E0 = ExpandWithDedup(J0, "APP", AppCols, "__APP"),

    // ============================================================
    // Join 2: APP -> ATSNO
    // ============================================================
    WithATSLINKID =
        Table.AddColumn(
            E0,
            "__ATSLINK_FOR_JOIN",
            each
                let
                    v = FirstUsefulField(_, {"ATSLINKID", "ATSLINKID__APP"})
                in
                    ToNumberOrNull(v),
            type number
        ),

    J1 =
        Table.NestedJoin(
            WithATSLINKID,
            {"__ATSLINK_FOR_JOIN"},
            ATSNO4_Distinct,
            {"__ATSLINK_KEY"},
            "ATSNO",
            JoinKind.LeftOuter
        ),

    AtsNoCols = Table.ColumnNames(ATSNO4_Distinct),
    E1 = ExpandWithDedup(J1, "ATSNO", AtsNoCols, "__ATSNO"),

    // ============================================================
    // Join 3: ONLY use ATSNUMBER.ATSAUTHORIZATIONNUMBER -> AUTH
    // ============================================================
    AUTH3 =
        Table.TransformColumns(
            AUTH2,
            {{"AUTHORIZATION_ID", each ToNumberOrNull(_), type number}}
        ),

    AUTH3_Filtered =
        Table.SelectRows(AUTH3, each [AUTHORIZATION_ID] <> null),

    // choose one AUTH row per AUTHORIZATION_ID, prefer latest updated row
    AUTH3_Sorted =
        Table.Sort(
            AUTH3_Filtered,
            {
                {"AUTHORIZATION_ID", Order.Ascending},
                {"WHEN_UPDATED", Order.Descending},
                {"ADJUDICATION_DATE", Order.Descending},
                {"APPLICATION_ACCEPTED_DATE", Order.Descending}
            }
        ),

    AUTH3_Unique =
        Table.Distinct(
            Table.Buffer(AUTH3_Sorted),
            {"AUTHORIZATION_ID"}
        ),

    WithAuthKey =
        Table.AddColumn(
            E1,
            "__AUTH_ID_FOR_JOIN",
            each ToNumberOrNull(FirstUsefulField(_, {"ATSAUTHORIZATIONNUMBER", "ATSAUTHORIZATIONNUMBER__ATSNO"})),
            type number
        ),

    AuthCols = Table.ColumnNames(AUTH3_Unique),

    J2 =
        Table.NestedJoin(
            WithAuthKey,
            {"__AUTH_ID_FOR_JOIN"},
            AUTH3_Unique,
            {"AUTHORIZATION_ID"},
            "AUTH",
            JoinKind.LeftOuter
        ),

    E2 = ExpandWithDedup(J2, "AUTH", AuthCols, "__AUTH"),

    // ============================================================
    // Add PIESID (job-number table)
    // ============================================================

    WithPIESID =
        Table.AddColumn(
            E2,
            "PIESID",
            each
                if IsNullOrBlank([JOBNUMBER])
                and not IsNullOrBlank([VFCTRACKING_NUMBER])
                then Text.From([VFCTRACKING_NUMBER])
                else null,
            type text
        ),

    Ranked0 =
        Table.AddColumn(
            WithPIESID,
            "__HAS_VFCT",
            each if not IsNullOrBlank([VFCTRACKING_NUMBER]) then 1 else 0,
            Int64.Type
        ),

    Ranked1 =
        Table.AddColumn(
            Ranked0,
            "__HAS_APP",
            each if not IsNullOrBlank(FirstUsefulField(_, {"JOBID", "JOBID__APP"})) then 1 else 0,
            Int64.Type
        ),

    Ranked2 =
        Table.AddColumn(
            Ranked1,
            "__HAS_ATS",
            each if not IsNullOrBlank(FirstUsefulField(_, {"ATSAUTHORIZATIONNUMBER", "ATSAUTHORIZATIONNUMBER__ATSNO"})) then 1 else 0,
            Int64.Type
        ),

    Ranked3 =
        Table.AddColumn(
            Ranked2,
            "__HAS_AUTH",
            each if not IsNullOrBlank(FirstUsefulField(_, {"AUTHORIZATION_ID__AUTH"})) then 1 else 0,
            Int64.Type
        ),

    Ranked4 =
        Table.AddColumn(
            Ranked3,
            "__AUTH_UPDATED_SORT",
            each ToDateTimeOrNull(FirstUsefulField(_, {"WHEN_UPDATED"})),
            type datetime
        ),

    Ranked5 =
        Table.AddColumn(
            Ranked4,
            "__ADJUDICATION_SORT",
            each ToDateTimeOrNull(FirstUsefulField(_, {"ADJUDICATION_DATE"})),
            type datetime
        ),

    Ranked6 =
        Table.AddColumn(
            Ranked5,
            "__ATS_AUTH_NUM_SORT",
            each ToNumberOrNull(FirstUsefulField(_, {"ATSAUTHORIZATIONNUMBER", "ATSAUTHORIZATIONNUMBER__ATSNO"})),
            type number
        ),

    PreferredSorted =
        Table.Sort(
            Ranked6,
            {
                {"PIESID", Order.Ascending},
                {"__HAS_VFCT", Order.Descending},
                {"__HAS_APP", Order.Descending},
                {"__HAS_ATS", Order.Descending},
                {"__HAS_AUTH", Order.Descending},
                {"__AUTH_UPDATED_SORT", Order.Descending},
                {"__ADJUDICATION_SORT", Order.Descending},
                {"__ATS_AUTH_NUM_SORT", Order.Descending},
                {"UNIQUEID", Order.Ascending}
            }
        ),

    OneRowPerPIESID =
        Table.Distinct(
            Table.Buffer(PreferredSorted),
            {"PIESID"}
        ),

    // ============================================================
    // Final derived fields (same business logic / same output names)
    // ============================================================
    S0  = EnsureIfMissing(OneRowPerPIESID, "CREATEDDATE", each FirstUsefulField(_, {"CREATEDDATE"}), type any),
    S1  = EnsureIfMissing(S0,        "SUBMITTEDDATE", each FirstUsefulField(_, {"SUBMITTEDDATE"}), type any),

    S2  = EnsureIfMissing(S1, "APPLICATION_ACCEPTED_DATE",
            each FirstUsefulField(_, {"APPLICATION_ACCEPTED_DATE", "FCBCACCEPTEDDATE", "RECEIVEDDATE"}),
            type any),

    S3  = EnsureIfMissing(S2, "APPLICATION_RECEIVED_DATE",
            each FirstUsefulField(_, {"APPLICATION_RECEIVED_DATE", "RECEIVEDDATE"}),
            type any),

    S4  = EnsureIfMissing(S3, "APPLICATION_REJECTED_DATE",
            each FirstUsefulField(_, {"APPLICATION_REJECTED_DATE"}),
            type any),

    S5  = EnsureIfMissing(S4, "AUTHORIZATION_STATUS_CODE",
            each FirstUsefulField(_, {"AUTHORIZATION_STATUS_CODE"}),
            type any),

    S6  = EnsureIfMissing(S5, "ATHN_CLOSE_REASON_CODE",
            each FirstUsefulField(_, {"ATHN_CLOSE_REASON_CODE"}),
            type any),

    S7  = EnsureIfMissing(S6, "ADJUDICATION_DATE",
            each FirstUsefulField(_, {"ADJUDICATION_DATE", "GRANTEDREFUSEDATE", "ATSDECISIONDATE"}),
            type any),

    S8  = EnsureIfMissing(S7, "FIRST_NATION_START_DATE",
            each FirstUsefulField(_, {"FIRST_NATION_START_DATE"}),
            type any),

    S9  = EnsureIfMissing(S8, "FIRST_NATION_COMPLETION_DATE",
            each FirstUsefulField(_, {"FIRST_NATION_COMPLETION_DATE"}),
            type any),

    S10 = EnsureIfMissing(S9, "ADJUDICATED_DAT",
            each FirstUsefulField(_, {"ADJUDICATED_DAT", "ADJUDICATION_DATE"}),
            type any),

    S11 = EnsureIfMissing(S10, "LAND_STATUS_DAT",
            each FirstUsefulField(_, {"LAND_STATUS_DAT", "TECH_REVIEW_COMPLETION_DATE", "TECHASSESSMENTCOMPLETEDATE"}),
            type any),

    S12 = EnsureIfMissing(S11, "REPORTED_DAT",
            each FirstUsefulField(_, {"REPORTED_DAT", "FCBC_PROCESS_COMPLETE_DATE", "FCBCCOMPLETEDATE", "GRANTEDREFUSEDATE"}),
            type any),

    S13 = EnsureIfMissing(S12, "DISALLOWED_DAT",
            each FirstUsefulField(_, {"DISALLOWED_DAT", "APPLICATION_REJECTED_DATE"}),
            type any),

    S14 = EnsureIfMissing(S13, "OFFERED_DAT", each FirstUsefulField(_, {"OFFERED_DAT"}), type any),
    S15 = EnsureIfMissing(S14, "OFFER_NOT_ACCEPTED_DAT", each FirstUsefulField(_, {"OFFER_NOT_ACCEPTED_DAT"}), type any),
    S16 = EnsureIfMissing(S15, "COMMENCEMENT_DAT", each FirstUsefulField(_, {"COMMENCEMENT_DAT"}), type any),
    S17 = EnsureIfMissing(S16, "CANCELED_DAT", each FirstUsefulField(_, {"CANCELED_DAT"}), type any),
    S18 = EnsureIfMissing(S17, "EXPIRY_DAT", each FirstUsefulField(_, {"EXPIRY_DAT", "AUTHORIZATION_DUE_DATE"}), type any),

    S19 = EnsureIfMissing(S18, "CODE_CHR_STATUS", each null, type any),

    S20 = EnsureIfMissing(
            S19,
            "CODE_CHR_STATUS != 'CA'",
            each
                let v = FirstUsefulField(_, {"CODE_CHR_STATUS"})
                in if v = null or IsNullOrBlank(v) then "OK" else Text.From(v),
            type text
         ),

    S21 = EnsureIfMissing(
            S20,
            "CODE_CHR_STATUS != 'DI'",
            each
                let v = FirstUsefulField(_, {"CODE_CHR_STATUS"})
                in if v = null or IsNullOrBlank(v) then "OK" else Text.From(v),
            type text
         ),

    // ============================================================
    // Remove helper columns only
    // ============================================================
    Cleaned =
        Table.RemoveColumns(
            S21,
            List.Intersect({
                Table.ColumnNames(S21),
                {
                    "__VFCT_KEY",
                    "__ATSLINK_KEY",
                    "__ATSLINK_FOR_JOIN",
                    "__AUTH_ID_FOR_JOIN",
                    "__HAS_VFCT",
                    "__HAS_APP",
                    "__HAS_ATS",
                    "__HAS_AUTH",
                    "__AUTH_UPDATED_SORT",
                    "__ADJUDICATION_SORT",
                    "__ATS_AUTH_NUM_SORT"
                }
            })
        ),

    // ============================================================
    // Final filter
    // ============================================================
    Output =
        Table.SelectRows(
            Cleaned,
            each [PIESID] <> null and Text.Trim(Text.From([PIESID])) <> ""
        )
in
    Output;
[BindToDefaultDestination = true]
shared pies_staging_water_authid = let
    // ============================================================
    // Parameters
    // ============================================================

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

    // ============================================================
    // Lakehouse navigation
    // ============================================================
    Source = Lakehouse.Contents(null),
    WS = Source{[workspaceId = WorkspaceId]}[Data],
    LH = WS{[lakehouseId = LakehouseId]}[Data],

    GetLHById = (id as text, friendly as text) as table =>
        let
            rows = Table.SelectRows(LH, each Record.HasFields(_, "Id") and [Id] = id),
            ranked =
                if Table.RowCount(rows) = 0 then
                    rows
                else
                    Table.AddColumn(
                        rows,
                        "__rank",
                        each if [ItemKind] = "Table" then 1 else if [ItemKind] = "View" then 2 else 9,
                        Int64.Type
                    ),
            best =
                if Table.RowCount(ranked) = 0 then
                    error ("Lakehouse object not found: " & friendly & " (Id=" & id & ")")
                else
                    Table.Sort(ranked, {{"__rank", Order.Ascending}}){0}[Data]
        in
            best,

    ATIS0       = GetLHById("wma_replication.atis_waterjob_combined_mv", "ATIS_WATERJOB_COMBINED_MV"),
    VFCBC_APP0  = GetLHById("vfcbc_replication.vfcbc_application_vw", "VFCBC_APPLICATION_VW"),
    VFCBC_ATS0  = GetLHById("vfcbc_replication.vfcbc_atsnumber_vw", "VFCBC_ATSNUMBER_VW"),
    ATS_AUTH0   = GetLHById("ats_replication.ats_authorizations", "ATS_AUTHORIZATIONS"),
    ONH0        = GetLHById("ats_replication.ats_authorizations_on_hold", "ATS_AUTHORIZATIONS_ON_HOLD"),

    // ============================================================
    // Helpers
    // ============================================================
    NormalizeCols = (t as table) as table =>
        Table.TransformColumnNames(t, each Text.Upper(Text.Trim(_))),

    IsNullOrBlank = (v as any) as logical =>
        v = null or Text.Trim(Text.From(v)) = "",

    EnsureCanonicalFromAliases = (t as table, canonical as text, aliases as list, tname as text) as table =>
        let
            cols = Table.ColumnNames(t),
            hit =
                if List.Contains(cols, canonical) then
                    canonical
                else
                    let found = List.Select(aliases, each List.Contains(cols, _))
                    in if List.Count(found) > 0 then found{0} else null
        in
            if hit = null then
                error (tname & " missing required column '" & canonical & "'. Available columns: " & Text.Combine(cols, ", "))
            else if hit = canonical then
                t
            else
                Table.AddColumn(t, canonical, each Record.Field(_, hit), type any),

    // Safe “time-only” detector
    IsTimeOnlyText = (s as text) as logical =>
        let
            t = Text.Trim(s),
            hasColon = Text.Contains(t, ":"),
            hasDash  = Text.Contains(t, "-"),
            hasSlash = Text.Contains(t, "/"),
            hasT     = Text.Contains(t, "T"),
            hasComma = Text.Contains(t, ",")
        in
            hasColon and (not hasDash) and (not hasSlash) and (not hasT) and (not hasComma),

    CleanDateValue = (v as any) as any =>
        if v = null then
            null
        else if Value.Is(v, type time) or Value.Is(v, type duration) then
            null
        else if Value.Is(v, type text) and IsTimeOnlyText(v) then
            null
        else
            v,

    FirstUsefulField = (r as record, names as list) as any =>
        let
            vals = List.Transform(
                names,
                (n) =>
                    let raw = try Record.Field(r, n) otherwise null,
                        v = CleanDateValue(raw)
                    in
                        if v = null then null
                        else if Value.Is(v, type text) and Text.Trim(v) = "" then null
                        else v
            ),
            nn = List.RemoveNulls(vals)
        in
            if List.Count(nn) > 0 then nn{0} else null,

    ToKeyText = (v as any) as nullable text =>
        if v = null then null
        else
            let t = Text.Trim(Text.From(v))
            in if t = "" then null else t,

    ToNumberOrNull = (v as any) as nullable number =>
        try Number.From(v) otherwise null,


    ExpandWithDedup = (left as table, nestedCol as text, rightCols as list, suffix as text) as table =>
        let
            existing = Table.ColumnNames(left),
            newNames = List.Transform(rightCols, each if List.Contains(existing, _) then _ & suffix else _),
            expanded = Table.ExpandTableColumn(left, nestedCol, rightCols, newNames)
        in
            expanded,

    // ============================================================
    // Normalize column names
    // ============================================================
    ATIS1      = NormalizeCols(ATIS0),
    APP1       = NormalizeCols(VFCBC_APP0),
    ATSNO1     = NormalizeCols(VFCBC_ATS0),
    AUTH1      = NormalizeCols(ATS_AUTH0),
    ONH1       = NormalizeCols(ONH0),

    // ============================================================
    // Ensure join columns exist
    // ============================================================
    ATIS2 =
        EnsureCanonicalFromAliases(
            ATIS1,
            "VFCTRACKING_NUMBER",
            {"VFCTRACKING_NUMBER", "VFCTRACKINGNUMBER", "VFC_TRACKING_NUMBER", "VFC_TRACKINGNUMBER", "TRACKINGNUMBER"},
            "ATIS"
        ),

    APP2 =
        EnsureCanonicalFromAliases(
            APP1,
            "VFCTRACKING_NUMBER",
            {"VFCTRACKING_NUMBER", "VFCTRACKINGNUMBER", "VFC_TRACKING_NUMBER", "VFC_TRACKINGNUMBER", "TRACKINGNUMBER"},
            "VFCBC_APPLICATION_VW"
        ),


    APP3 =
        EnsureCanonicalFromAliases(
            APP2,
            "ATSLINKID",
            {"ATSLINKID", "ATS_LINK_ID", "ATS_LINKID"},
            "VFCBC_APPLICATION_VW"
        ),

    ATSNO2 =
        EnsureCanonicalFromAliases(
            ATSNO1,
            "ATSLINKID",
            {"ATSLINKID", "ATS_LINK_ID", "ATS_LINKID"},
            "VFCBC_ATSNUMBER_VW"
        ),

    ATSNO3 =
        EnsureCanonicalFromAliases(
            ATSNO2,
            "ATSAUTHORIZATIONNUMBER",
            {"ATSAUTHORIZATIONNUMBER", "ATS_AUTHORIZATION_NUMBER", "ATSAUTHNUMBER"},
            "VFCBC_ATSNUMBER_VW"
        ),


    AUTH2 =
        EnsureCanonicalFromAliases(
            AUTH1,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATS_AUTHORIZATIONS"
        ),

    ONH2 =
        EnsureCanonicalFromAliases(
            ONH1,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATS_AUTHORIZATIONS_ON_HOLD"
        ),


    ATIS3 =
        EnsureCanonicalFromAliases(
            ATIS2,
            "AUTHORIZATION_ID",
            {"AUTHORIZATION_ID", "AUTHORIZATIONID"},
            "ATIS"
        ),


    ATIS4 = Table.AddColumn(ATIS3, "__VFCT_KEY", each ToKeyText([VFCTRACKING_NUMBER]), type text),
    APP4  = Table.AddColumn(APP3,  "__VFCT_KEY", each ToKeyText([VFCTRACKING_NUMBER]), type text),

    APP5  = Table.AddColumn(APP4,  "__ATSLINK_KEY", each ToKeyText([ATSLINKID]), type text),
    ATSNO4= Table.AddColumn(ATSNO3,"__ATSLINK_KEY", each ToKeyText([ATSLINKID]), type text),



    J0 =
        Table.NestedJoin(
            ATIS4,
            {"__VFCT_KEY"},
            APP5,
            {"__VFCT_KEY"},
            "APP",
            JoinKind.LeftOuter
        ),


    AppCols = Table.ColumnNames(APP5),
    E0 = ExpandWithDedup(J0, "APP", AppCols, "__APP"),

    // For ATSNO join
    WithATSLINKID =
        Table.AddColumn(
            E0,
            "__ATSLINK_FOR_JOIN",
            each
                let
                    v = FirstUsefulField(_, {"ATSLINKID", "ATSLINKID__APP"})
                in
                    ToKeyText(v),
            type text
        ),

    J1 =
        Table.NestedJoin(
            WithATSLINKID,
            {"__ATSLINK_FOR_JOIN"},
            ATSNO4,
            {"__ATSLINK_KEY"},
            "ATSNO",
            JoinKind.LeftOuter
        ),

    AtsNoCols = Table.ColumnNames(ATSNO4),
    E1 = ExpandWithDedup(J1, "ATSNO", AtsNoCols, "__ATSNO"),


    WithAuthKey =
        Table.AddColumn(
            E1,
            "__AUTH_ID_FOR_JOIN",
            each
                let
                    aRaw = FirstUsefulField(_, {"AUTHORIZATION_ID"}),
                    a = ToNumberOrNull(aRaw),
                    bRaw = FirstUsefulField(_, {"ATSAUTHORIZATIONNUMBER", "ATSAUTHORIZATIONNUMBER__ATSNO"}),
                    b = ToNumberOrNull(bRaw)
                in
                    if a <> null then a else b,
            type number
        ),

    AUTH3 = Table.TransformColumns(AUTH2, {{"AUTHORIZATION_ID", each ToNumberOrNull(_), type number}}),

    J2 =
        Table.NestedJoin(
            WithAuthKey,
            {"__AUTH_ID_FOR_JOIN"},
            AUTH3,
            {"AUTHORIZATION_ID"},
            "AUTH",
            JoinKind.LeftOuter
        ),

    AuthCols = Table.ColumnNames(AUTH3),
    E2 = ExpandWithDedup(J2, "AUTH", AuthCols, "__AUTH"),

    ONH3 = Table.TransformColumns(ONH2, {{"AUTHORIZATION_ID", each ToNumberOrNull(_), type number}}),

    J3 =
        Table.NestedJoin(
            E2,
            {"__AUTH_ID_FOR_JOIN"},
            ONH3,
            {"AUTHORIZATION_ID"},
            "ONH",
            JoinKind.LeftOuter
        ),

    OnhCols =
        {
            "AUTHORIZATION_ON_HOLD_ID",
            "ATHN_ON_HOLD_REASON_CODE",
            "CURRENT_ON_HOLD_IND",
            "ON_HOLD_START_DATE",
            "ON_HOLD_END_DATE",
            "ON_HOLD_NOTES",
            "REMOVED_BY",
            "PARTNER_AGENCY_ID"
        },

    E3 = ExpandWithDedup(J3, "ONH", OnhCols, "__ONH"),

    WithPIESID =
        Table.AddColumn(
            E2,
            "PIESID",
            each
                if IsNullOrBlank([JOBNUMBER])
                and not IsNullOrBlank([VFCTRACKING_NUMBER])
                then Text.From([VFCTRACKING_NUMBER])
                else null,
            type text
        ),

    EnsureIfMissing = (t as table, col as text, compute as function, colType as type) as table =>
        if List.Contains(Table.ColumnNames(t), col) then t else Table.AddColumn(t, col, compute, colType),


    S0  = EnsureIfMissing(WithPIESID, "CREATEDDATE", each FirstUsefulField(_, {"CREATEDDATE"}), type any),
    S1  = EnsureIfMissing(S0,        "SUBMITTEDDATE", each FirstUsefulField(_, {"SUBMITTEDDATE"}), type any),

    S2  = EnsureIfMissing(S1, "APPLICATION_ACCEPTED_DATE",
            each FirstUsefulField(_, {"APPLICATION_ACCEPTED_DATE", "FCBCACCEPTEDDATE", "RECEIVEDDATE"}),
            type any),

    S3  = EnsureIfMissing(S2, "APPLICATION_RECEIVED_DATE",
            each FirstUsefulField(_, {"APPLICATION_RECEIVED_DATE", "RECEIVEDDATE"}),
            type any),

    S4  = EnsureIfMissing(S3, "APPLICATION_REJECTED_DATE",
            each FirstUsefulField(_, {"APPLICATION_REJECTED_DATE"}),
            type any),

    S5  = EnsureIfMissing(S4, "AUTHORIZATION_STATUS_CODE",
            each FirstUsefulField(_, {"AUTHORIZATION_STATUS_CODE"}),
            type any),

    S6  = EnsureIfMissing(S5, "ATHN_CLOSE_REASON_CODE",
            each FirstUsefulField(_, {"ATHN_CLOSE_REASON_CODE"}),
            type any),

    S7  = EnsureIfMissing(S6, "ADJUDICATION_DATE",
            each FirstUsefulField(_, {"ADJUDICATION_DATE", "GRANTEDREFUSEDATE", "ATSDECISIONDATE"}),
            type any),

    S8  = EnsureIfMissing(S7, "FIRST_NATION_START_DATE",
            each FirstUsefulField(_, {"FIRST_NATION_START_DATE"}),
            type any),

    S9  = EnsureIfMissing(S8, "FIRST_NATION_COMPLETION_DATE",
            each FirstUsefulField(_, {"FIRST_NATION_COMPLETION_DATE"}),
            type any),


    S10 = EnsureIfMissing(S9, "ADJUDICATED_DAT",
            each FirstUsefulField(_, {"ADJUDICATED_DAT", "ADJUDICATION_DATE"}),
            type any),


    S11 = EnsureIfMissing(S10, "LAND_STATUS_DAT",
            each FirstUsefulField(_, {"LAND_STATUS_DAT", "TECH_REVIEW_COMPLETION_DATE", "TECHASSESSMENTCOMPLETEDATE"}),
            type any),

    S12 = EnsureIfMissing(S11, "REPORTED_DAT",
            each FirstUsefulField(_, {"REPORTED_DAT", "FCBC_PROCESS_COMPLETE_DATE", "FCBCCOMPLETEDATE", "GRANTEDREFUSEDATE"}),
            type any),

    S13 = EnsureIfMissing(S12, "DISALLOWED_DAT",
            each FirstUsefulField(_, {"DISALLOWED_DAT", "APPLICATION_REJECTED_DATE"}),
            type any),

    S14 = EnsureIfMissing(S13, "OFFERED_DAT", each FirstUsefulField(_, {"OFFERED_DAT"}), type any),
    S15 = EnsureIfMissing(S14, "OFFER_NOT_ACCEPTED_DAT", each FirstUsefulField(_, {"OFFER_NOT_ACCEPTED_DAT"}), type any),
    S16 = EnsureIfMissing(S15, "COMMENCEMENT_DAT", each FirstUsefulField(_, {"COMMENCEMENT_DAT"}), type any),
    S17 = EnsureIfMissing(S16, "CANCELED_DAT", each FirstUsefulField(_, {"CANCELED_DAT"}), type any),
    S18 = EnsureIfMissing(S17, "EXPIRY_DAT", each FirstUsefulField(_, {"EXPIRY_DAT", "AUTHORIZATION_DUE_DATE"}), type any),

    S19 = EnsureIfMissing(S18, "CODE_CHR_STATUS", each null, type any),

    S20 = EnsureIfMissing(
            S19,
            "CODE_CHR_STATUS != 'CA'",
            each
                let v = FirstUsefulField(_, {"CODE_CHR_STATUS"})
                in if v = null or IsNullOrBlank(v) then "OK" else Text.From(v),
            type text
         ),

    S21 = EnsureIfMissing(
            S20,
            "CODE_CHR_STATUS != 'DI'",
            each
                let v = FirstUsefulField(_, {"CODE_CHR_STATUS"})
                in if v = null or IsNullOrBlank(v) then "OK" else Text.From(v),
            type text
         ),


    Cleaned =
        Table.RemoveColumns(
            S21,
            List.Intersect({Table.ColumnNames(S21), {"__VFCT_KEY", "__ATSLINK_KEY", "__ATSLINK_FOR_JOIN", "__AUTH_ID_FOR_JOIN"}})
        ),

    Output =
        Table.SelectRows(Cleaned, each [PIESID] <> null and Text.Trim(Text.From([PIESID])) <> "")
in
    Output;
[BindToDefaultDestination = true]
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false])
    {[workspaceId = #"pmt-workspace-id"]}[Data]
    {[lakehouseId = #"pmt-lakehouse-id"]}[Data];
shared #"DefaultDestination (2)" = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "60f10f69-06b8-44a3-a3cf-1afd52d4d754"]}[Data]{[lakehouseId = "964899a6-7e35-47cd-9eb3-429482e1b733"]}[Data];
