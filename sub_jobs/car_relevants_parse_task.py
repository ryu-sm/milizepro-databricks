# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        cars_data = []

        car_replacements_data = []

        # 自動車
        for car in orig_profile.get("自動車", []):
            cars_data.append(
                CarModel(
                    **{
                        **car,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )

        # 自動車買替
        car_replacements_data.append(
            CarReplacementModel(
                **{
                    **orig_profile,
                    "profile_id": new_profile_id,
                }
            ).dict()
        )

        if cars_data:
            spark.createDataFrame(cars_data, CarStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.cars")
        if car_replacements_data:
            spark.createDataFrame(
                car_replacements_data, CarReplacementStruct
            ).write.mode("append").saveAsTable(
                f"{configs.DELTA_SCHEMA}.car_replacements"
            )

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id,
                name="car_relevants_parse_task",
                error_message=str(e),
            ).dict()
        )
        raise e
