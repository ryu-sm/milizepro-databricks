# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        brief_insurances_data = []

        detailed_insurances_data = []

        # 簡易保険
        for brief_insurance in orig_profile.get("保険", []):
            brief_insurances_data.append(
                BriefInsuranceModel(
                    **{
                        **brief_insurance,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )

        insurance_details = get_insurance_details(profile_orig_id)

        for detailed_insurance in insurance_details:
            detailed_insurances_data.append(
                DetailedInsuranceModel(
                    **{
                        **detailed_insurance,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )


        if brief_insurances_data:
            spark.createDataFrame(brief_insurances_data, BriefInsuranceStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.brief_insurances")
        if detailed_insurances_data:
            spark.createDataFrame(detailed_insurances_data, DetailedInsuranceStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.detailed_insurances")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id,
                name="insurance_relevants_parse_task",
                error_message=str(e),
            ).dict()
        )
        raise e
