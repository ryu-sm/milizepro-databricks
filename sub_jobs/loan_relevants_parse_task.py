# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        profile_data = ProfileModel(
            **{**orig_profile, "id": new_profile_id, "orig_id": profile_orig_id}
        )

        brief_loans_data = []

        brief_loan_repayment_details_data = []

        detailed_loans_data = []

        loan_early_repayment_details_data = []

        for loan in orig_profile.get("支出_ローンイベント", []):
            brief_loan = BriefLoanModel(
                **{
                    **loan,
                    "profile_id": new_profile_id,
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )
            brief_loans_data.append(brief_loan.dict())

            for age, amount in loan.get("loan", {}).items():
                brief_loan_repayment_details_data.append(
                    BriefLoansRepaymentDetailModel(
                        **{
                            "brief_loan_id": brief_loan.id,
                            "age": age,
                            "amount": amount,
                            "created_at": orig_profile.get("created_at"),
                            "updated_at": orig_profile.get("updated_at"),
                        }
                    ).dict()
                )


        for loan in orig_profile.get("支出_ローンイベント詳細", []):
            detail_loan = DetailedInsuranceModel(
                **{
                    **loan,
                    "profile_id": new_profile_id,
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )
            detailed_loans_data.append(detail_loan.dict())

            for detail in loan.get("繰上返済情報", []):
                loan_early_repayment_details_data.append(
                    LoanEarlyRepaymentDetailModel(
                        **{
                            "detailed_loan_id": detail_loan.id,
                            **detail,
                            "created_at": orig_profile.get("created_at"),
                            "updated_at": orig_profile.get("updated_at"),
                        }
                    ).dict()
                )

        if brief_loans_data:
            spark.createDataFrame(brief_loans_data, BriefLoanStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.brief_loans")
        if brief_loan_repayment_details_data:
            spark.createDataFrame(brief_loan_repayment_details_data, BriefLoansRepaymentDetailStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.brief_loan_repayment_details")
        if detailed_loans_data:
            spark.createDataFrame(detailed_loans_data, DetailedLoanStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.detailed_loans")
        if loan_early_repayment_details_data:
            spark.createDataFrame(loan_early_repayment_details_data, LoanEarlyRepaymentDetailStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.loan_early_repayment_details")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="loan_relevants_parse_task", error_message=str(e)
            ).dict()
        )
        raise e
