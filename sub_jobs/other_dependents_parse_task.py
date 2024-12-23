# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        other_dependents_data = []

        for other_dependent in orig_profile.get("その他扶養家族", []):
            other_dependents_data.append(
                OtherDependentModel(
                    **{
                        **other_dependent,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )

        spark.createDataFrame(other_dependents_data, OtherDependentStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.other_dependents")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="other_dependents_parse_task", error_message=str(e)
            ).dict()
        )
        raise e
