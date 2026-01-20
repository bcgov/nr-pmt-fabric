[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data]{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data];
[BindToDefaultDestination = true]
shared ATIS_WATERJOB_COMBINED_MV = let
    Source = Lakehouse.Contents(null),
    Navigation = Source{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],
    #"Navigation 1" = Navigation{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Id = "ATIS_WATERJOB_COMBINED_MV", ItemKind = "Table"]}[Data],

    // Get current column names
    CurrentColumns = Table.ColumnNames(#"Navigation 2"),
    // Transform to uppercase
    UpperColumns = List.Transform(CurrentColumns, each Text.Upper(_)),
    // Rename all columns in bulk
    #"Renamed to Uppercase" = Table.RenameColumns(
        #"Navigation 2",
        List.Zip({CurrentColumns, UpperColumns})
    )
in
    #"Renamed to Uppercase";
[BindToDefaultDestination = true]
shared #"ats_replication ats_authorizations" = let
    Source = Lakehouse.Contents(null),
    Navigation = Source{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],
    #"Navigation 1" = Navigation{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Id = "ats_replication ats_authorizations", ItemKind = "Table"]}[Data],

    // Get current column names
    CurrentColumns = Table.ColumnNames(#"Navigation 2"),
    // Transform to uppercase
    UpperColumns = List.Transform(CurrentColumns, each Text.Upper(_)),
    // Rename all columns in bulk
    #"Renamed to Uppercase" = Table.RenameColumns(
        #"Navigation 2",
        List.Zip({CurrentColumns, UpperColumns})
    )
in
    #"Renamed to Uppercase";
[BindToDefaultDestination = true]
shared VFCBC_APPLICATION_VW = let
    Source = Lakehouse.Contents(null),
    Navigation = Source{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],
    #"Navigation 1" = Navigation{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data],
    #"Navigation 2" = #"Navigation 1"{[Id = "VFCBC_APPLICATION_VW", ItemKind = "Table"]}[Data],

    // Clean RECEIVEDDATE — set to null if year < 1900
    #"Cleaned RECEIVEDDATE" =
        Table.TransformColumns(
            #"Navigation 2",
            {
                {
                    "RECEIVEDDATE",
                    each if Date.Year(_) < 1900 then null else _
                }
            }
        ),

    // Clean other date columns the same way (add as needed)
    #"Cleaned CREATEDDATE" =
        Table.TransformColumns(
            #"Cleaned RECEIVEDDATE",
            {
                {
                    "CREATEDDATE",
                    each if Date.Year(_) < 1900 then null else _
                }
            }
        )
in
    #"Cleaned CREATEDDATE";
[BindToDefaultDestination = true]
shared VFCBC_ATSNUMBER_VW = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "79dafa8a-b966-4240-80f7-2e9d46baa5c3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "VFCBC_ATSNUMBER_VW", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared waterjobview_leftjoin_vfcbc_application = let
  Source = Table.NestedJoin(ATIS_WATERJOB_COMBINED_MV, {"VFCTRACKING_NUMBER"}, VFCBC_APPLICATION_VW, {"VFCTRACKING_NUMBER"}, "VFCBC_APPLICATION_VW", JoinKind.LeftOuter),
  #"Expanded VFCBC_APPLICATION_VW" = Table.ExpandTableColumn(Source, "VFCBC_APPLICATION_VW", {"JOBID", "VFCTRACKING_NUMBER", "BUSINESSAREANUMBER", "ATSLINKID", "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER", "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER", "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS", "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT", "SUBMITTEDDATE", "RECEIVEDDATE", "DATECOMPLETED"}, {"JOBID", "VFCTRACKING_NUMBER.1", "BUSINESSAREANUMBER", "ATSLINKID", "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER", "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER", "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS", "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT", "SUBMITTEDDATE", "RECEIVEDDATE.1", "DATECOMPLETED"})
in
  #"Expanded VFCBC_APPLICATION_VW";
[BindToDefaultDestination = true]
shared query_jobnumber_piesid = let
    // 1. ATIS × VFCBC_APPLICATION_VW on VFCTRACKING_NUMBER
    Source0 =
        Table.NestedJoin(
            ATIS_WATERJOB_COMBINED_MV,
            {"VFCTRACKING_NUMBER"},
            VFCBC_APPLICATION_VW,
            {"VFCTRACKING_NUMBER"},
            "VFCBC_APPLICATION_VW",
            JoinKind.LeftOuter
        ),

    #"Expanded VFCBC_APPLICATION_VW" =
        Table.ExpandTableColumn(
            Source0,
            "VFCBC_APPLICATION_VW",
            {
                "JOBID", "VFCTRACKING_NUMBER", "BUSINESSAREANUMBER", "ATSLINKID",
                "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER",
                "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER",
                "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS",
                "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT",
                "SUBMITTEDDATE", "RECEIVEDDATE", "DATECOMPLETED"
            },
            {
                "JOBID", "VFCTRACKING_NUMBER.1", "BUSINESSAREANUMBER", "ATSLINKID",
                "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER",
                "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER",
                "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS",
                "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT",
                "SUBMITTEDDATE", "RECEIVEDDATE.1", "DATECOMPLETED"
            }
        ),

    // 2. Join VFCBC_ATSNUMBER_VW on ATSLINKID
    Source1 =
        Table.NestedJoin(
            #"Expanded VFCBC_APPLICATION_VW",
            {"ATSLINKID"},
            VFCBC_ATSNUMBER_VW,
            {"ATSLINKID"},
            "VFCBC_ATSNUMBER_VW",
            JoinKind.LeftOuter
        ),

    #"Expanded VFCBC_ATSNUMBER_VW" =
        Table.ExpandTableColumn(
            Source1,
            "VFCBC_ATSNUMBER_VW",
            {
                "ATSLINKID", "ATSAUTHORIZATIONNUMBER",
                "LANDSFILENUMBER", "DISPOSITION_TRANSACTION_SID",
                "DISPOSITION_TRANSACTION_STRING"
            },
            {
                "ATSLINKID.1", "ATSAUTHORIZATIONNUMBER",
                "LANDSFILENUMBER.1", "DISPOSITION_TRANSACTION_SID",
                "DISPOSITION_TRANSACTION_STRING"
            }
        ),

    // 3. Build join key for AUTH table (AUTHORIZATION_ID or ATSAUTHORIZATIONNUMBER)
    WithAuthKey =
        Table.AddColumn(
            #"Expanded VFCBC_ATSNUMBER_VW",
            "AUTH_ID_FOR_JOIN",
            each if [AUTHORIZATION_ID] <> null
                 then [AUTHORIZATION_ID]
                 else [ATSAUTHORIZATIONNUMBER]
        ),

    // 4. Join ats_replication ats_authorizations
    Source2 =
        Table.NestedJoin(
            WithAuthKey,
            {"AUTH_ID_FOR_JOIN"},
            #"ats_replication ats_authorizations",
            {"AUTHORIZATION_ID"},
            "ats_replication ats_authorizations",
            JoinKind.LeftOuter
        ),

    #"Expanded ats_replication ats_authorizations" =
        Table.ExpandTableColumn(
            Source2,
            "ats_replication ats_authorizations",
            {
                "AUTHORIZATION_ID", "APPLICATION_ACCEPTED_DATE", "APPLICATION_RECEIVED_DATE",
                "APPLICATION_FEE", "APPLICATION_EGARMS_RECEIPT", "AUTHORIZATION_DUE_DATE",
                "AUTHORIZATION_FEE", "AUTHORIZATION_EGARMS_RECEIPT", "AMENDMENT_RENEWAL_DATE",
                "TARGET_DAYS", "BRING_FORWARD_DATE", "TERMS",
                "AGCY_REPRESENTATIVE_FIRST_NAME", "AGENCY_REPRESENTATIVE_SURNAME",
                "VALUE_OFFERED_COMMENT", "TENURE_ADMINISTRATION_LEAD",
                "TECH_REVIEW_COMPLETION_DATE", "FIRST_NATION_COMMENT",
                "FIRST_NATION_START_DATE", "FIRST_NATION_COMPLETION_DATE",
                "FIRST_NATION_CONSULTATION_LEAD", "ADJUDICATION_DATE",
                "FCBC_PROCESS_COMPLETE_DATE", "AUTHORIZATION_STATUS_CODE",
                "AUTHORIZATION_PAYMENT_TYPE_CD", "APPLICATION_PAYMENT_TYPE_CODE",
                "ATHN_CLOSE_REASON_CODE", "PROJECT_ID", "MANAGING_FCBC_REGION_ID",
                "REGIONAL_USER_ID", "AUTHORIZATION_INSTRUMENT_ID",
                "APPLICATION_REJECTED_DATE", "AUTHORIZATION_TARGET_ID",
                "SUBREGIONAL_OFFICE_ID", "SECONDARY_OFFICE_ID", "FILE_NUMBER",
                "WHO_CREATED", "WHEN_CREATED", "WHO_UPDATED", "WHEN_UPDATED",
                "CONSULTATION_ACTIVITY_ID", "CONSULTATION_STATE",
                "CONSULTATION_START_DATE", "CONSULTATION_COMPLETION_DATE"
            },
            {
                "AUTHORIZATION_ID.1", "APPLICATION_ACCEPTED_DATE", "APPLICATION_RECEIVED_DATE",
                "APPLICATION_FEE", "APPLICATION_EGARMS_RECEIPT", "AUTHORIZATION_DUE_DATE",
                "AUTHORIZATION_FEE", "AUTHORIZATION_EGARMS_RECEIPT", "AMENDMENT_RENEWAL_DATE",
                "TARGET_DAYS", "BRING_FORWARD_DATE", "TERMS",
                "AGCY_REPRESENTATIVE_FIRST_NAME", "AGENCY_REPRESENTATIVE_SURNAME",
                "VALUE_OFFERED_COMMENT", "TENURE_ADMINISTRATION_LEAD",
                "TECH_REVIEW_COMPLETION_DATE", "FIRST_NATION_COMMENT",
                "FIRST_NATION_START_DATE", "FIRST_NATION_COMPLETION_DATE",
                "FIRST_NATION_CONSULTATION_LEAD", "ADJUDICATION_DATE",
                "FCBC_PROCESS_COMPLETE_DATE", "AUTHORIZATION_STATUS_CODE",
                "AUTHORIZATION_PAYMENT_TYPE_CD", "APPLICATION_PAYMENT_TYPE_CODE",
                "ATHN_CLOSE_REASON_CODE", "PROJECT_ID", "MANAGING_FCBC_REGION_ID",
                "REGIONAL_USER_ID", "AUTHORIZATION_INSTRUMENT_ID",
                "APPLICATION_REJECTED_DATE", "AUTHORIZATION_TARGET_ID",
                "SUBREGIONAL_OFFICE_ID", "SECONDARY_OFFICE_ID", "FILE_NUMBER",
                "WHO_CREATED", "WHEN_CREATED", "WHO_UPDATED", "WHEN_UPDATED",
                "CONSULTATION_ACTIVITY_ID", "CONSULTATION_STATE",
                "CONSULTATION_START_DATE", "CONSULTATION_COMPLETION_DATE"
            }
        ),

    // 5. Take only rows where JOBNUMBER has a value (non-null)
    FilteredRows =
        Table.SelectRows(
            #"Expanded ats_replication ats_authorizations",
            each [JOBNUMBER] <> null
        ),

    // 6. Rename JOBNUMBER → PIESID
    RenamedPIESID =
        Table.RenameColumns(FilteredRows, {{"JOBNUMBER", "PIESID"}})
in
    RenamedPIESID;
[BindToDefaultDestination = true]
shared query_trackingnumber_piesid = let
    // 1. ATIS × VFCBC_APPLICATION_VW on VFCTRACKING_NUMBER
    Source0 =
        Table.NestedJoin(
            ATIS_WATERJOB_COMBINED_MV,
            {"VFCTRACKING_NUMBER"},
            VFCBC_APPLICATION_VW,
            {"VFCTRACKING_NUMBER"},
            "VFCBC_APPLICATION_VW",
            JoinKind.LeftOuter
        ),

    #"Expanded VFCBC_APPLICATION_VW" =
        Table.ExpandTableColumn(
            Source0,
            "VFCBC_APPLICATION_VW",
            {
                "JOBID", "VFCTRACKING_NUMBER", "BUSINESSAREANUMBER", "ATSLINKID",
                "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER",
                "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER",
                "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS",
                "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT",
                "SUBMITTEDDATE", "RECEIVEDDATE", "DATECOMPLETED"
            },
            {
                "JOBID", "VFCTRACKING_NUMBER.1", "BUSINESSAREANUMBER", "ATSLINKID",
                "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER",
                "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER",
                "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS",
                "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT",
                "SUBMITTEDDATE", "RECEIVEDDATE.1", "DATECOMPLETED"
            }
        ),

    // 2. Join VFCBC_ATSNUMBER_VW on ATSLINKID
    Source1 =
        Table.NestedJoin(
            #"Expanded VFCBC_APPLICATION_VW",
            {"ATSLINKID"},
            VFCBC_ATSNUMBER_VW,
            {"ATSLINKID"},
            "VFCBC_ATSNUMBER_VW",
            JoinKind.LeftOuter
        ),

    #"Expanded VFCBC_ATSNUMBER_VW" =
        Table.ExpandTableColumn(
            Source1,
            "VFCBC_ATSNUMBER_VW",
            {
                "ATSLINKID", "ATSAUTHORIZATIONNUMBER",
                "LANDSFILENUMBER", "DISPOSITION_TRANSACTION_SID",
                "DISPOSITION_TRANSACTION_STRING"
            },
            {
                "ATSLINKID.1", "ATSAUTHORIZATIONNUMBER",
                "LANDSFILENUMBER.1", "DISPOSITION_TRANSACTION_SID",
                "DISPOSITION_TRANSACTION_STRING"
            }
        ),

    // 3. Build join key for AUTH table (AUTHORIZATION_ID or ATSAUTHORIZATIONNUMBER)
    WithAuthKey =
        Table.AddColumn(
            #"Expanded VFCBC_ATSNUMBER_VW",
            "AUTH_ID_FOR_JOIN",
            each if [AUTHORIZATION_ID] <> null
                 then [AUTHORIZATION_ID]
                 else [ATSAUTHORIZATIONNUMBER]
        ),

    // 4. Join ats_replication ats_authorizations
    Source2 =
        Table.NestedJoin(
            WithAuthKey,
            {"AUTH_ID_FOR_JOIN"},
            #"ats_replication ats_authorizations",
            {"AUTHORIZATION_ID"},
            "ats_replication ats_authorizations",
            JoinKind.LeftOuter
        ),

    #"Expanded ats_replication ats_authorizations" =
        Table.ExpandTableColumn(
            Source2,
            "ats_replication ats_authorizations",
            {
                "AUTHORIZATION_ID", "APPLICATION_ACCEPTED_DATE", "APPLICATION_RECEIVED_DATE",
                "APPLICATION_FEE", "APPLICATION_EGARMS_RECEIPT", "AUTHORIZATION_DUE_DATE",
                "AUTHORIZATION_FEE", "AUTHORIZATION_EGARMS_RECEIPT", "AMENDMENT_RENEWAL_DATE",
                "TARGET_DAYS", "BRING_FORWARD_DATE", "TERMS",
                "AGCY_REPRESENTATIVE_FIRST_NAME", "AGENCY_REPRESENTATIVE_SURNAME",
                "VALUE_OFFERED_COMMENT", "TENURE_ADMINISTRATION_LEAD",
                "TECH_REVIEW_COMPLETION_DATE", "FIRST_NATION_COMMENT",
                "FIRST_NATION_START_DATE", "FIRST_NATION_COMPLETION_DATE",
                "FIRST_NATION_CONSULTATION_LEAD", "ADJUDICATION_DATE",
                "FCBC_PROCESS_COMPLETE_DATE", "AUTHORIZATION_STATUS_CODE",
                "AUTHORIZATION_PAYMENT_TYPE_CD", "APPLICATION_PAYMENT_TYPE_CODE",
                "ATHN_CLOSE_REASON_CODE", "PROJECT_ID", "MANAGING_FCBC_REGION_ID",
                "REGIONAL_USER_ID", "AUTHORIZATION_INSTRUMENT_ID",
                "APPLICATION_REJECTED_DATE", "AUTHORIZATION_TARGET_ID",
                "SUBREGIONAL_OFFICE_ID", "SECONDARY_OFFICE_ID", "FILE_NUMBER",
                "WHO_CREATED", "WHEN_CREATED", "WHO_UPDATED", "WHEN_UPDATED",
                "CONSULTATION_ACTIVITY_ID", "CONSULTATION_STATE",
                "CONSULTATION_START_DATE", "CONSULTATION_COMPLETION_DATE"
            },
            {
                "AUTHORIZATION_ID.1", "APPLICATION_ACCEPTED_DATE", "APPLICATION_RECEIVED_DATE",
                "APPLICATION_FEE", "APPLICATION_EGARMS_RECEIPT", "AUTHORIZATION_DUE_DATE",
                "AUTHORIZATION_FEE", "AUTHORIZATION_EGARMS_RECEIPT", "AMENDMENT_RENEWAL_DATE",
                "TARGET_DAYS", "BRING_FORWARD_DATE", "TERMS",
                "AGCY_REPRESENTATIVE_FIRST_NAME", "AGENCY_REPRESENTATIVE_SURNAME",
                "VALUE_OFFERED_COMMENT", "TENURE_ADMINISTRATION_LEAD",
                "TECH_REVIEW_COMPLETION_DATE", "FIRST_NATION_COMMENT",
                "FIRST_NATION_START_DATE", "FIRST_NATION_COMPLETION_DATE",
                "FIRST_NATION_CONSULTATION_LEAD", "ADJUDICATION_DATE",
                "FCBC_PROCESS_COMPLETE_DATE", "AUTHORIZATION_STATUS_CODE",
                "AUTHORIZATION_PAYMENT_TYPE_CD", "APPLICATION_PAYMENT_TYPE_CODE",
                "ATHN_CLOSE_REASON_CODE", "PROJECT_ID", "MANAGING_FCBC_REGION_ID",
                "REGIONAL_USER_ID", "AUTHORIZATION_INSTRUMENT_ID",
                "APPLICATION_REJECTED_DATE", "AUTHORIZATION_TARGET_ID",
                "SUBREGIONAL_OFFICE_ID", "SECONDARY_OFFICE_ID", "FILE_NUMBER",
                "WHO_CREATED", "WHEN_CREATED", "WHO_UPDATED", "WHEN_UPDATED",
                "CONSULTATION_ACTIVITY_ID", "CONSULTATION_STATE",
                "CONSULTATION_START_DATE", "CONSULTATION_COMPLETION_DATE"
            }
        ),

    // 5. JOBNUMBER is null AND VFCTRACKING_NUMBER has value
    FilteredRows =
        Table.SelectRows(
            #"Expanded ats_replication ats_authorizations",
            each [JOBNUMBER] = null and [VFCTRACKING_NUMBER] <> null
        ),

    // 6. VFCTRACKING_NUMBER → PIESID
    RenamedPIESID =
        Table.RenameColumns(FilteredRows, {{"VFCTRACKING_NUMBER", "PIESID"}})
in
    RenamedPIESID;
[BindToDefaultDestination = true]
shared query_authorizationid_piesid = let
    // 1. ATIS × VFCBC_APPLICATION_VW on VFCTRACKING_NUMBER
    Source0 =
        Table.NestedJoin(
            ATIS_WATERJOB_COMBINED_MV,
            {"VFCTRACKING_NUMBER"},
            VFCBC_APPLICATION_VW,
            {"VFCTRACKING_NUMBER"},
            "VFCBC_APPLICATION_VW",
            JoinKind.LeftOuter
        ),

    #"Expanded VFCBC_APPLICATION_VW" =
        Table.ExpandTableColumn(
            Source0,
            "VFCBC_APPLICATION_VW",
            {
                "JOBID", "VFCTRACKING_NUMBER", "BUSINESSAREANUMBER", "ATSLINKID",
                "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER",
                "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER",
                "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS",
                "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT",
                "SUBMITTEDDATE", "RECEIVEDDATE", "DATECOMPLETED"
            },
            {
                "JOBID", "VFCTRACKING_NUMBER.1", "BUSINESSAREANUMBER", "ATSLINKID",
                "ATSPROJECTNUMBER", "EXTERNALREFERENCE", "EXTERNALJOBNUMBER",
                "EXTERNALJOBNUMBER2", "MINENUMBER", "LANDSFILENUMBER",
                "AUTHORIZATIONTYPE", "APPLICATIONTYPE", "CREATEDDATE", "STATUS",
                "OFFICE", "BUSINESSAREA", "SUBMITTEDBYTYPE", "STATUSSHOWNTOCLIENT",
                "SUBMITTEDDATE", "RECEIVEDDATE.1", "DATECOMPLETED"
            }
        ),

    // 2. Join VFCBC_ATSNUMBER_VW on ATSLINKID
    Source1 =
        Table.NestedJoin(
            #"Expanded VFCBC_APPLICATION_VW",
            {"ATSLINKID"},
            VFCBC_ATSNUMBER_VW,
            {"ATSLINKID"},
            "VFCBC_ATSNUMBER_VW",
            JoinKind.LeftOuter
        ),

    #"Expanded VFCBC_ATSNUMBER_VW" =
        Table.ExpandTableColumn(
            Source1,
            "VFCBC_ATSNUMBER_VW",
            {
                "ATSLINKID", "ATSAUTHORIZATIONNUMBER",
                "LANDSFILENUMBER", "DISPOSITION_TRANSACTION_SID",
                "DISPOSITION_TRANSACTION_STRING"
            },
            {
                "ATSLINKID.1", "ATSAUTHORIZATIONNUMBER",
                "LANDSFILENUMBER.1", "DISPOSITION_TRANSACTION_SID",
                "DISPOSITION_TRANSACTION_STRING"
            }
        ),

    // 3. Build join key: AUTHORIZATION_ID or ATSAUTHORIZATIONNUMBER
    WithAuthKey =
        Table.AddColumn(
            #"Expanded VFCBC_ATSNUMBER_VW",
            "AUTH_ID_FOR_JOIN",
            each if [AUTHORIZATION_ID] <> null
                 then [AUTHORIZATION_ID]
                 else [ATSAUTHORIZATIONNUMBER]
        ),

    // 4. Join ats_replication ats_authorizations
    Source2 =
        Table.NestedJoin(
            WithAuthKey,
            {"AUTH_ID_FOR_JOIN"},
            #"ats_replication ats_authorizations",
            {"AUTHORIZATION_ID"},
            "ats_replication ats_authorizations",
            JoinKind.LeftOuter
        ),

    #"Expanded ats_replication ats_authorizations" =
        Table.ExpandTableColumn(
            Source2,
            "ats_replication ats_authorizations",
            {
                "AUTHORIZATION_ID", "APPLICATION_ACCEPTED_DATE", "APPLICATION_RECEIVED_DATE",
                "APPLICATION_FEE", "APPLICATION_EGARMS_RECEIPT", "AUTHORIZATION_DUE_DATE",
                "AUTHORIZATION_FEE", "AUTHORIZATION_EGARMS_RECEIPT", "AMENDMENT_RENEWAL_DATE",
                "TARGET_DAYS", "BRING_FORWARD_DATE", "TERMS",
                "AGCY_REPRESENTATIVE_FIRST_NAME", "AGENCY_REPRESENTATIVE_SURNAME",
                "VALUE_OFFERED_COMMENT", "TENURE_ADMINISTRATION_LEAD",
                "TECH_REVIEW_COMPLETION_DATE", "FIRST_NATION_COMMENT",
                "FIRST_NATION_START_DATE", "FIRST_NATION_COMPLETION_DATE",
                "FIRST_NATION_CONSULTATION_LEAD", "ADJUDICATION_DATE",
                "FCBC_PROCESS_COMPLETE_DATE", "AUTHORIZATION_STATUS_CODE",
                "AUTHORIZATION_PAYMENT_TYPE_CD", "APPLICATION_PAYMENT_TYPE_CODE",
                "ATHN_CLOSE_REASON_CODE", "PROJECT_ID", "MANAGING_FCBC_REGION_ID",
                "REGIONAL_USER_ID", "AUTHORIZATION_INSTRUMENT_ID",
                "APPLICATION_REJECTED_DATE", "AUTHORIZATION_TARGET_ID",
                "SUBREGIONAL_OFFICE_ID", "SECONDARY_OFFICE_ID", "FILE_NUMBER",
                "WHO_CREATED", "WHEN_CREATED", "WHO_UPDATED", "WHEN_UPDATED",
                "CONSULTATION_ACTIVITY_ID", "CONSULTATION_STATE",
                "CONSULTATION_START_DATE", "CONSULTATION_COMPLETION_DATE"
            },
            {
                "AUTHORIZATION_ID.1", "APPLICATION_ACCEPTED_DATE", "APPLICATION_RECEIVED_DATE",
                "APPLICATION_FEE", "APPLICATION_EGARMS_RECEIPT", "AUTHORIZATION_DUE_DATE",
                "AUTHORIZATION_FEE", "AUTHORIZATION_EGARMS_RECEIPT", "AMENDMENT_RENEWAL_DATE",
                "TARGET_DAYS", "BRING_FORWARD_DATE", "TERMS",
                "AGCY_REPRESENTATIVE_FIRST_NAME", "AGENCY_REPRESENTATIVE_SURNAME",
                "VALUE_OFFERED_COMMENT", "TENURE_ADMINISTRATION_LEAD",
                "TECH_REVIEW_COMPLETION_DATE", "FIRST_NATION_COMMENT",
                "FIRST_NATION_START_DATE", "FIRST_NATION_COMPLETION_DATE",
                "FIRST_NATION_CONSULTATION_LEAD", "ADJUDICATION_DATE",
                "FCBC_PROCESS_COMPLETE_DATE", "AUTHORIZATION_STATUS_CODE",
                "AUTHORIZATION_PAYMENT_TYPE_CD", "APPLICATION_PAYMENT_TYPE_CODE",
                "ATHN_CLOSE_REASON_CODE", "PROJECT_ID", "MANAGING_FCBC_REGION_ID",
                "REGIONAL_USER_ID", "AUTHORIZATION_INSTRUMENT_ID",
                "APPLICATION_REJECTED_DATE", "AUTHORIZATION_TARGET_ID",
                "SUBREGIONAL_OFFICE_ID", "SECONDARY_OFFICE_ID", "FILE_NUMBER",
                "WHO_CREATED", "WHEN_CREATED", "WHO_UPDATED", "WHEN_UPDATED",
                "CONSULTATION_ACTIVITY_ID", "CONSULTATION_STATE",
                "CONSULTATION_START_DATE", "CONSULTATION_COMPLETION_DATE"
            }
        ),

    // 5. JOBNUMBER null AND VFCTRACKING_NUMBER null AND AUTH_ID_FOR_JOIN has value
    FilteredRows =
        Table.SelectRows(
            #"Expanded ats_replication ats_authorizations",
            each [JOBNUMBER] = null
                 and [VFCTRACKING_NUMBER] = null
                 and [AUTH_ID_FOR_JOIN] <> null
        ),

    // 6. AUTH_ID_FOR_JOIN → PIESID
    RenamedPIESID =
        Table.RenameColumns(FilteredRows, {{"AUTH_ID_FOR_JOIN", "PIESID"}})
in
    RenamedPIESID;
