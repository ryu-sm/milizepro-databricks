# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        spendings_data = []

        spending_increases_data = []

        extraordinaries_data = []

        # 支出
        user_spendings = get_user_spendings(profile_orig_id)
        if user_spendings:
            for ym, spending in user_spendings.get("spending_data", {}).items():
                spendings_data.append(
                    SpendingModel(
                        **{
                            **spending,
                            "profile_id": new_profile_id,
                            "ym": ym,
                            "calc_setting": user_spendings.get("calc_setting"),
                            "created_at": user_spendings.get("created_at"),
                            "updated_at": user_spendings.get("updated_at"),
                        }
                    ).dict()
                )

            spark.createDataFrame(spendings_data, SpendingStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.spendings")


        # 支出変動
        user_outgo_rates = get_user_outgo_rates(profile_orig_id)
        if user_outgo_rates:
            for user_outgo_rate in user_outgo_rates:
                spending_increases_data.append(
                    SpendingIncreaseModel(
                        **{
                            **user_outgo_rate,
                            "profile_id": new_profile_id,
                        }
                    ).dict()
                )
            
            spark.createDataFrame(spending_increases_data, SpendingIncreaseStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.spending_increases")
        

        # 特別支出
        for extraordinary in orig_profile.get("支出_ライフイベント", []):
            extraordinaries_data.append(
                ExtraordinaryModel(
                    **{
                        **extraordinary,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )

        spark.createDataFrame(extraordinaries_data, ExtraordinaryStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.extraordinaries")


        # リタイア後支出
        spending_retirement_data = SpendingRetirementModel(
            **{**orig_profile, "profile_id": new_profile_id}
        )
        spark.createDataFrame([spending_retirement_data.dict()], SpendingRetirementStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.spending_retirements")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id,
                name="spending_relevants_parse_task",
                error_message=str(e),
            ).dict()
        )
        raise e
