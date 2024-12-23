# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        salaries_data = []
        other_salaries_data = []
        salary_increases_data = []

        # 収入
        salaries_data.append(
            SalaryModel(
                **{
                    "profile_id": new_profile_id,
                    "category": "本人",
                    "annual_income": orig_profile.get("収入_給与"),
                    "bonus": orig_profile.get("収入_給与_賞与分"),
                    "expected_retirement_age": orig_profile.get("退職想定年齢"),
                    "expected_retirement_amount": orig_profile.get("退職金想定額"),
                    "retirement_receiving_age": orig_profile.get("退職金受給年齢"),
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            ).dict()
        )
        salaries_data.append(
            SalaryModel(
                **{
                    "profile_id": new_profile_id,
                    "category": "配偶者",
                    "annual_income": orig_profile.get("収入_配偶者給与"),
                    "bonus": orig_profile.get("収入_配偶者給与_賞与分"),
                    "expected_retirement_age": orig_profile.get("配偶者退職想定年齢"),
                    "expected_retirement_amount": orig_profile.get("配偶者退職金想定額"),
                    "retirement_receiving_age": orig_profile.get("配偶者退職金受給年齢"),
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            ).dict()
        )

        spark.createDataFrame(salaries_data, SalaryStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.salaries")

        # その他の収入
        for other_salary in orig_profile.get("その他の収入", []):
            other_salaries_data.append(
                OtherSalaryModel(
                    **{
                        **other_salary,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )

        spark.createDataFrame(other_salaries_data, OtherSalaryStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.other_salaries")

        # 収入変動
        user_salary_increase = get_user_salary_increases(profile_orig_id)
        if user_salary_increase:
            for person_increase in user_salary_increase.get("person_increase", []):
                salary_increases_data.append(
                    SalaryIncreaseModel(
                        **{
                            **person_increase,
                            "profile_id": new_profile_id,
                            "category": "本人",
                            "created_at": user_salary_increase.get("created_at"),
                            "updated_at": user_salary_increase.get("updated_at"),
                        }
                    ).dict()
                )
            for spouse_increase in user_salary_increase.get("spouse_increase", []):
                salary_increases_data.append(
                    SalaryIncreaseModel(
                        **{
                            **spouse_increase,
                            "profile_id": new_profile_id,
                            "category": "配偶者",
                            "created_at": user_salary_increase.get("created_at"),
                            "updated_at": user_salary_increase.get("updated_at"),
                        }
                    ).dict()
                )

        spark.createDataFrame(salary_increases_data, SalaryIncreaseStruct).write.mode(
            "append"
        ).saveAsTable(f"{configs.DELTA_SCHEMA}.salary_increases")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="salary_relevants_parse_task", error_message=str(e)
            ).dict()
        )
        raise e
