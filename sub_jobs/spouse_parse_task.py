# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        spouse_data = SpouseModel(**{**orig_profile, "profile_id": new_profile_id})

        spark.createDataFrame([spouse_data.dict()], SpouseStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.spouses")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="spouse_parse_task", error_message=str(e)
            ).dict()
        )
        raise e
