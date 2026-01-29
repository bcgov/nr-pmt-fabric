[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = "79dafa8a-b966-4240-80f7-2e9d46baa5c3" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared #"pmt-lakehouse-id" = "201b9b7d-ca11-46b6-81e8-feeba4c99642" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
[BindToDefaultDestination = true]
shared pies_staging_water_jobnumber = let
    // ============================================================
    // Parameters
    // ============================================================
    WorkspaceId = Text.From(#"pmt-workspace-id"),
    LakehouseId = Text.From(#"pmt-lakehouse-id"),

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


    WithPIESID =
        Table.AddColumn(
            E2,
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
    // Parameters
    // ============================================================
    WorkspaceId = Text.From(#"pmt-workspace-id"),
    LakehouseId = Text.From(#"pmt-lakehouse-id"),

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
shared pies_staging_water_authid = let
    // ============================================================
    // Parameters
    // ============================================================
    WorkspaceId = Text.From(#"pmt-workspace-id"),
    LakehouseId = Text.From(#"pmt-lakehouse-id"),

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


    WithPIESID =
        Table.AddColumn(
            E2,
            "PIESID",
            each
                let
                    ats = FirstUsefulField(_, {
                        "ATSAUTHORIZATIONNUMBER",
                        "ATSAUTHORIZATIONNUMBER__ATSNO"
                    })
                in
                    if IsNullOrBlank([JOBNUMBER])
                    and IsNullOrBlank([VFCTRACKING_NUMBER])
                    and not IsNullOrBlank(ats)
                    then Text.From(ats)
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
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "60f10f69-06b8-44a3-a3cf-1afd52d4d754"]}[Data]{[lakehouseId = "964899a6-7e35-47cd-9eb3-429482e1b733"]}[Data];
