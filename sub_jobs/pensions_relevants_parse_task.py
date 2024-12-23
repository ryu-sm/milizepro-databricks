# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        public_pensions_data = []

        pension_fixeds_data = []

        pension_regular_flights_data = []

        pension_resumes_data = []

        corporate_pensions_data = []

        for _type in ["UserPersonPension", "UserSpousePension"]:
            pension = get_user_pensions(profile_orig_id, _type)
            if pension:
                public_pension = PublicPensionModel(
                    **{
                        "category": _type,
                        "method": pension.get("regular_or_resume"),
                        "profile_id": new_profile_id,
                        "created_at": pension.get("created_at"),
                        "updated_at": pension.get("updated_at"),
                    }
                )
                public_pensions_data.append(public_pension.dict())

                pension_fixeds_data.append(
                    PensionFixedModel(
                        **{
                            "public_pension_id": public_pension.id,
                            **pension.get("pension_fixed", {}),
                            "created_at": pension.get("created_at"),
                            "updated_at": pension.get("updated_at"),
                        }
                    ).dict()
                )

                pension_regular_flight = pension.get("pension_regular_flights", {})
                pension_regular_flights_data.append(
                    PensionRegularFlightModel(
                        **{
                            "public_pension_id": public_pension.id,
                            **pension_regular_flight,
                            "national_pension": pension_regular_flight.get(
                                "national_pension", {}
                            ).get("basic"),
                            "employees_pension_general": pension_regular_flight.get(
                                "employees_pension", {}
                            ).get("general"),
                            "employees_pension_public": pension_regular_flight.get(
                                "employees_pension", {}
                            ).get("public"),
                            "employees_pension_mutual": pension_regular_flight.get(
                                "employees_pension", {}
                            ).get("mutual"),
                            "created_at": pension.get("created_at"),
                            "updated_at": pension.get("updated_at"),
                        }
                    ).dict()
                )

                pension_resume = pension.get("pension_resume", {})
                for resume in pension_resume.get("resume_ary", []):
                    pension_resumes_data.append(
                        PensionResumeModel(
                            **{
                                "public_pension_id": public_pension.id,
                                "start_age": pension_resume.get("default_age"),
                                "start_date": f'{resume.get("start_year")}/{resume.get("start_month")}',
                                "end_date": f'{resume.get("end_year")}/{resume.get("end_month")}',
                                "income": resume.get("income"),
                                "bonus": resume.get("bonus"),
                                "created_at": pension.get("created_at"),
                                "updated_at": pension.get("updated_at"),
                            }
                        ).dict()
                    )

                corporate_pension = pension.get("corporate_pension", {})
                if corporate_pension:
                    corporate_pension_pension_fund_association = corporate_pension.get(
                        "pension_fund_association", {}
                    )
                    if corporate_pension_pension_fund_association:
                        corporate_pensions_data.append(
                            CorporatePensionModel(
                                **{
                                    "profile_id": new_profile_id,
                                    "category": _type,
                                    "type": "その他の企業年金",
                                    **corporate_pension_pension_fund_association,
                                    "created_at": pension.get("created_at"),
                                    "updated_at": pension.get("updated_at"),
                                }
                            ).dict()
                        )

                    for db in corporate_pension.get("dc", []):
                        corporate_pensions_data.append(
                            CorporatePensionModel(
                                **{
                                    "profile_id": new_profile_id,
                                    "category": _type,
                                    "type": "確定給付企業年金",
                                    **db,
                                    "created_at": pension.get("created_at"),
                                    "updated_at": pension.get("updated_at"),
                                }
                            ).dict()
                        )

                    for dc in corporate_pension.get("dc", []):
                        corporate_pensions_data.append(
                            CorporatePensionModel(
                                **{
                                    "profile_id": new_profile_id,
                                    "category": _type,
                                    "type": "企業型確定拠出年金",
                                    **dc,
                                    "created_at": pension.get("created_at"),
                                    "updated_at": pension.get("updated_at"),
                                }
                            ).dict()
                        )

        if public_pensions_data:
            spark.createDataFrame(public_pensions_data, PublicPensionStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.public_pensions")
        if pension_fixeds_data:
            spark.createDataFrame(pension_fixeds_data, PensionFixedStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.pension_fixeds")
        if pension_regular_flights_data:
            spark.createDataFrame(pension_regular_flights_data, PensionRegularFlightStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.pension_regular_flights")
        if pension_resumes_data:
            spark.createDataFrame(pension_resumes_data, PensionResumeStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.pension_resumes")
        if corporate_pensions_data:
            spark.createDataFrame(corporate_pensions_data, CorporatePensionStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.corporate_pensions")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id,
                name="pensions_relevants_parse_task",
                error_message=str(e),
            ).dict()
        )
        raise e
