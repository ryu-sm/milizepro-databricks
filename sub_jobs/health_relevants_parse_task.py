# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        health_checks = get_health_checks(profile_orig_id)

        health_indicators_data = []

        if health_checks:
            for date, status in health_checks.get("daily_status", {}).items():
                print(date)
                if status:
                    health_indicators_data.append(
                        HealthIndicatorModel(
                            **{
                                **status,
                                "profile_id": new_profile_id,
                                "date": date,
                                "blood_type": health_checks.get("blood_type"),
                                "created_at": health_checks.get("created_at"),
                                "updated_at": health_checks.get("updated_at"),
                            }
                        ).dict()
                    )
        
        if health_indicators_data:
            spark.createDataFrame(health_indicators_data, HealthIndicatorStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.health_indicators")
    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id,
                name="health_relevants_parse_task",
                error_message=str(e),
            ).dict()
        )
        raise e
