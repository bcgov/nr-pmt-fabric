[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared #"pmt-workspace-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt_env_vars/pmt-workspace-id)",
        "60f10f69-06b8-44a3-a3cf-1afd52d4d754")
in
  Source;
shared #"pmt-lakehouse-id" = let
  Source = Variable.ValueOrDefault(
        "$(/**/pmt_env_vars/pmt-lakehouse-id)",
        "964899a6-7e35-47cd-9eb3-429482e1b733")
in
  Source;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "pies_staging_lands_vfcbc_only_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared pies_staging_lands_vfcbc_only = let
    Source = Lakehouse.Contents(null),
    WS     = Source{[workspaceId = #"pmt-workspace-id"]}[Data],
    LH     = WS{[lakehouseId  = #"pmt-lakehouse-id"]}[Data],

    SourceView = LH{[Id = "tantalis_staging.tantalis_vfcbc_only_vw"]}[Data],

    RequiredColumns = {
        "PIESID",
        "system_id",

        "CREATEDDATE",
        "SUBMITTEDDATE",
        "APPLICATION_RECEIVED_DATE",
        "APPLICATION_ACCEPTED_DATE",
        "APPLICATION_REJECTED_DATE",
        "ADJUDICATION_DATE",
        "ADJUDICATED_DAT",
        "DISALLOWED_DAT",
        "OFFERED_DAT",
        "OFFER_NOT_ACCEPTED_DAT",
        "COMPLETED_DAT",
        "CANCELED_DAT",
        "EXPIRY_DAT",
        "LAND_STATUS_DAT",
        "REPORTED_DAT",

        "FIRST_NATION_START_DATE",
        "FIRST_NATION_COMPLETION_DATE",

        "AUTHORIZATION_STATUS_CODE",
        "ATHN_CLOSE_REASON_CODE",
        "CODE_CHR_STATUS",

        "ATSAUTHORIZATIONNUMBER",

        "JOBID",
        "APPLICANT",
        "SUBMITTER",
        "AUTHORIZATIONTYPE",
        "APPLICATIONTYPE",

        "APPLICATION_TYPE_CDE",
        "STAGE_NME",
        "TYPE_NME",
        "SUBTYPE_NME",
        "PURPOSE_NME",
        "SUBPURPOSE_NME",

        "STATUS_V",
        "STATUS_VISIBLE_V",
        "ACTIVATION_CDE",
        "STATUS_NME",
        "REASON_DSC",

        "TRACKINGNUMBER",
        "ATSLINKID",
        "ATSAUTHORIZATIONNUMBER_RAW",
        "ATSPROJECTNUMBER",
        "LANDSFILENUMBER",

        "DISPOSITION_TRANSACTION_STRING",
        "DISPOSITION_TRANSACTION_SID_V",
        "DISPOSITION_SID",
        "DISPOSITION_TRANSACTION_SID",
        "REPLMNT_FOR_DISP_TRANS_SID",
        "FILE_CHR",

        "BUSINESSAREA",
        "BUSINESSAREANUMBER",
        "ORG_UNIT_SID",
        "MANAGING_AGENCY",

        "OFFICE",
        "UNIT_NAME",
        "DISTRICT_OFFICE",

        "CREATEDDATE_V",
        "SUBMITTEDDATE_V",
        "RECEIVEDDATE_V",
        "DATECOMPLETED_V",

        "RECEIVED_DAT",
        "COMMENCEMENT_DAT",
        "EFFECTIVE_DAT",
        "REVIEW_DAT",

        "LAND_STATUS_DAT_RAW",
        "REPORTED_DAT_RAW",
        "ADJUDICATED_DAT_RAW",
        "DISALLOWED_DAT_RAW",
        "OFFERED_DAT_RAW",
        "OFFER_NOT_ACCEPTED_DAT_RAW",
        "EXPIRY_DAT_RAW",
        "CANCELED_DAT_RAW",

        "CODE_CHR_STAGE",
        "CODE_CHR_STATUS_RAW",
        "CODE_CHR_REASON"
    },

    SelectedColumns = Table.SelectColumns(SourceView, RequiredColumns),

    ChangedTypes = Table.TransformColumnTypes(
        SelectedColumns,
        {
            {"PIESID", type text},
            {"system_id", type text},

            {"CREATEDDATE", type datetime},
            {"SUBMITTEDDATE", type datetime},
            {"APPLICATION_RECEIVED_DATE", type datetime},
            {"APPLICATION_ACCEPTED_DATE", type datetime},
            {"APPLICATION_REJECTED_DATE", type datetime},
            {"ADJUDICATION_DATE", type datetime},
            {"ADJUDICATED_DAT", type datetime},
            {"DISALLOWED_DAT", type datetime},
            {"OFFERED_DAT", type datetime},
            {"OFFER_NOT_ACCEPTED_DAT", type datetime},
            {"COMPLETED_DAT", type datetime},
            {"CANCELED_DAT", type datetime},
            {"EXPIRY_DAT", type datetime},
            {"LAND_STATUS_DAT", type datetime},
            {"REPORTED_DAT", type datetime},

            {"FIRST_NATION_START_DATE", type datetime},
            {"FIRST_NATION_COMPLETION_DATE", type datetime},

            {"AUTHORIZATION_STATUS_CODE", type text},
            {"ATHN_CLOSE_REASON_CODE", type text},
            {"CODE_CHR_STATUS", type text},

            {"ATSAUTHORIZATIONNUMBER", type text},

            {"JOBID", type text},
            {"APPLICANT", type text},
            {"SUBMITTER", type text},
            {"AUTHORIZATIONTYPE", type text},
            {"APPLICATIONTYPE", type text},

            {"APPLICATION_TYPE_CDE", type text},
            {"STAGE_NME", type text},
            {"TYPE_NME", type text},
            {"SUBTYPE_NME", type text},
            {"PURPOSE_NME", type text},
            {"SUBPURPOSE_NME", type text},

            {"STATUS_V", type text},
            {"STATUS_VISIBLE_V", type text},
            {"ACTIVATION_CDE", type text},
            {"STATUS_NME", type text},
            {"REASON_DSC", type text},

            {"TRACKINGNUMBER", type text},
            {"ATSLINKID", type text},
            {"ATSAUTHORIZATIONNUMBER_RAW", type text},
            {"ATSPROJECTNUMBER", type text},
            {"LANDSFILENUMBER", type text},

            {"DISPOSITION_TRANSACTION_STRING", type text},
            {"DISPOSITION_TRANSACTION_SID_V", Int64.Type},
            {"DISPOSITION_SID", Int64.Type},
            {"DISPOSITION_TRANSACTION_SID", Int64.Type},
            {"REPLMNT_FOR_DISP_TRANS_SID", Int64.Type},
            {"FILE_CHR", type text},

            {"BUSINESSAREA", type text},
            {"BUSINESSAREANUMBER", type text},
            {"ORG_UNIT_SID", Int64.Type},
            {"MANAGING_AGENCY", type text},

            {"OFFICE", type text},
            {"UNIT_NAME", type text},
            {"DISTRICT_OFFICE", type text},

            {"CREATEDDATE_V", type datetime},
            {"SUBMITTEDDATE_V", type datetime},
            {"RECEIVEDDATE_V", type datetime},
            {"DATECOMPLETED_V", type datetime},

            {"RECEIVED_DAT", type datetime},
            {"COMMENCEMENT_DAT", type datetime},
            {"EFFECTIVE_DAT", type datetime},
            {"REVIEW_DAT", type datetime},

            {"LAND_STATUS_DAT_RAW", type datetime},
            {"REPORTED_DAT_RAW", type datetime},
            {"ADJUDICATED_DAT_RAW", type datetime},
            {"DISALLOWED_DAT_RAW", type datetime},
            {"OFFERED_DAT_RAW", type datetime},
            {"OFFER_NOT_ACCEPTED_DAT_RAW", type datetime},
            {"EXPIRY_DAT_RAW", type datetime},
            {"CANCELED_DAT_RAW", type datetime},

            {"CODE_CHR_STAGE", type text},
            {"CODE_CHR_STATUS_RAW", type text},
            {"CODE_CHR_REASON", type text}
        },
        "en-CA"
    )
in
    ChangedTypes;
shared pies_staging_lands_vfcbc_only_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = #"pmt-workspace-id"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = #"pmt-lakehouse-id"]}[Data],
  Navigation_3 = Navigation_2{[Id = "tantalis_staging", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "pies_staging_lands_vfcbc_only", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
