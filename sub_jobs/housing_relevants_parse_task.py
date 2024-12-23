# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        housing_rentals_data = []

        housings_data = []

        ownership_details_fluctuations_data = []

        home_loans_data = []

        repayment_scenario_details_data = []

        home_loan_early_repayment_details_data = []

        # 賃貸
        for housing_rental in orig_profile.get("住居_賃貸", []):
            housing_rentals_data.append(
                HousingRentalModel(
                    **{
                        **housing_rental,
                        "profile_id": new_profile_id,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                ).dict()
            )

        # 住居_自己所有
        for housing in orig_profile.get("住居_自己所有", []):
            housing_hased = HousingModel(
                **{
                    "profile_id": new_profile_id,
                    "category": "自己所有",
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )
            housings_data.append(housing_hased.dict())

            for ownership_details_fluctuation in housing.get(
                "ownership_details_fluctuation", []
            ):
                ownership_details_fluctuation_data = OwnershipDetailsFluctuationModel(
                    **{
                        "housing_id": housing_hased.id,
                        **ownership_details_fluctuation,
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )
                ownership_details_fluctuations_data.append(
                    ownership_details_fluctuation_data.dict()
                )

            orig_home_loan = get_home_loans(
                profile_orig_id,
                "OwnershipHomeLoan",
                int(housing.get("sub_home", 0))
            )
            if orig_home_loan:
                home_loan = HomeLoanModel(
                    **{
                        **orig_home_loan,
                        "profile_id": new_profile_id,
                        "housing_id": housing_hased.id,
                    }
                )
                home_loans_data.append(home_loan.dict())
                for payment_date, repayment_scenario_detail_basic in orig_home_loan.get(
                    "返済シナリオ", {}
                ).items():
                    repayment_scenario_details_data.append(
                        RepaymentScenarioDetailModel(
                            **{
                                "home_loan_id": home_loan.id,
                                "payment_date": payment_date,
                                **repayment_scenario_detail_basic,
                                "created_at": orig_home_loan.get("created_at"),
                                "updated_at": orig_home_loan.get("updated_at"),
                            }
                        ).dict()
                    )
                for home_loan_early_repayment_detail in orig_home_loan.get("繰上返済情報", []):
                    home_loan_early_repayment_details_data.append(
                        HomeLoanEarlyRepaymentDetailModel(
                            **{
                                "home_loan_id": home_loan.id,
                                **home_loan_early_repayment_detail,
                                "created_at": orig_home_loan.get("created_at"),
                                "updated_at": orig_home_loan.get("updated_at"),
                            }
                        ).dict()
                    )

        # 住居_取得予定
        for housing in orig_profile.get("住居_取得予定", []):

            housing_hased = HousingModel(
                    **{
                        "profile_id": new_profile_id,
                        "category": "取得予定",
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )
            housings_data.append(
                housing_hased.dict()
            )
            orig_home_loan = get_home_loans(
                profile_orig_id, "PlanHomeLoan", int(housing.get("sub_home", 0))
            )
            if orig_home_loan:
                home_loan = HomeLoanModel(
                    **{
                        **orig_home_loan,
                        "profile_id": new_profile_id,
                        "housing_id": housing_hased.id,
                    }
                )
                home_loans_data.append(
                    home_loan.dict()
                )
                for payment_date, repayment_scenario_detail_basic in orig_home_loan.get(
                    "返済シナリオ", {}
                ).items():
                    repayment_scenario_details_data.append(
                        RepaymentScenarioDetailModel(
                            **{
                                "home_loan_id": home_loan.id,
                                "payment_date": payment_date,
                                **repayment_scenario_detail_basic,
                                "created_at": orig_home_loan.get("created_at"),
                                "updated_at": orig_home_loan.get("updated_at"),
                            }
                        ).dict()
                    )
                for home_loan_early_repayment_detail in orig_home_loan.get("繰上返済情報", []):
                    home_loan_early_repayment_details_data.append(
                        HomeLoanEarlyRepaymentDetailModel(
                            **{
                                "home_loan_id": home_loan.id,
                                **home_loan_early_repayment_detail,
                                "created_at": orig_home_loan.get("created_at"),
                                "updated_at": orig_home_loan.get("updated_at"),
                            }
                        ).dict()
                    )

        if housing_rentals_data:
            spark.createDataFrame(housing_rentals_data, HousingRentalStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.housing_rentals")
        if housings_data:
            spark.createDataFrame(housings_data, HousingStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.housings")
        if ownership_details_fluctuations_data:
            spark.createDataFrame(ownership_details_fluctuations_data, OwnershipDetailsFluctuationStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.ownership_details_fluctuations")
        if repayment_scenario_details_data:
            spark.createDataFrame(repayment_scenario_details_data, RepaymentScenarioDetailStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.repayment_scenario_details")
        if home_loans_data:
            spark.createDataFrame(home_loans_data, HomeLoanStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.home_loans")
        if home_loan_early_repayment_details_data:
            spark.createDataFrame(home_loan_early_repayment_details_data, HomeLoanEarlyRepaymentDetailStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.home_loan_early_repayment_details")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="housing_relevants_parse_task", error_message=str(e)
            ).dict()
        )
        raise e
