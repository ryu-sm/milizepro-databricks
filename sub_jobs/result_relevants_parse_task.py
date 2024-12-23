# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        result = get_chart_histories(profile_orig_id)

        cashflows_data = []

        financial_asset_charts_data = []

        if result:
            import json
            print(json.dumps(result.get("cashflow_data", {}), indent=4))
            for index in range(
                0,
                len(
                    result.get("cashflow_data", {})
                    .get("cashflow", {})
                    .get("age_householder", [])
                ),
            ):
                # キャッシュフロー
                age_householder =result.get("cashflow_data", {}).get("cashflow", {}).get("age_householder", [])
                age_spouse =result.get("cashflow_data", {}).get("cashflow", {}).get("age_spouse", [])
                income_householder =result.get("cashflow_data", {}).get("cashflow", {}).get("income_householder", [])
                pension_householder =result.get("cashflow_data", {}).get("cashflow", {}).get("pension_householder", [])
                income_spouse =result.get("cashflow_data", {}).get("cashflow", {}).get("income_spouse", [])
                pension_spouse =result.get("cashflow_data", {}).get("cashflow", {}).get("pension_spouse", [])
                income_insurance =result.get("cashflow_data", {}).get("cashflow", {}).get("income_insurance", [])
                income_other =result.get("cashflow_data", {}).get("cashflow", {}).get("income_other", [])
                income_sum =result.get("cashflow_data", {}).get("cashflow", {}).get("income_sum", [])
                expense_living =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_living", [])
                expense_insurance =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_insurance", [])
                expense_home =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_home", [])
                repayment_home =result.get("cashflow_data", {}).get("cashflow", {}).get("repayment_home", [])
                expense_child =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_child", [])
                expense_car =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_car", [])
                tax_and_social_ins =result.get("cashflow_data", {}).get("cashflow", {}).get("tax_and_social_ins", [])
                home_loan_deduction =result.get("cashflow_data", {}).get("cashflow", {}).get("home_loan_deduction", [])
                repayment_other =result.get("cashflow_data", {}).get("cashflow", {}).get("repayment_other", [])
                expense_event =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_event", [])
                expense_sum =result.get("cashflow_data", {}).get("cashflow", {}).get("expense_sum", [])
                assets_liquidity =result.get("cashflow_data", {}).get("cashflow", {}).get("assets_liquidity", [])
                assets_under_mgmt =result.get("cashflow_data", {}).get("cashflow", {}).get("assets_under_mgmt", [])
                homeloans_balance =result.get("cashflow_data", {}).get("cashflow", {}).get("homeloans_balance", [])
                death_benefit =result.get("cashflow_data", {}).get("cashflow", {}).get("death_benefit", [])
                financial_asset_purchase =result.get("cashflow_data", {}).get("cashflow", {}).get("financial_asset_purchase", [])
                assets_ideco =result.get("cashflow_data", {}).get("cashflow", {}).get("assets_ideco", [])
                assets_normal =result.get("cashflow_data", {}).get("cashflow", {}).get("assets_normal", [])
                assets_real_estate =result.get("cashflow_data", {}).get("cashflow", {}).get("assets_real_estate", [])
                assets_insurance =result.get("cashflow_data", {}).get("cashflow", {}).get("assets_insurance", [])
                car_loan_balance =result.get("cashflow_data", {}).get("cashflow", {}).get("car_loan_balance", [])
                other_loan_balance =result.get("cashflow_data", {}).get("cashflow", {}).get("other_loan_balance", [])
                nisa_balance =result.get("cashflow_data", {}).get("cashflow", {}).get("nisa_balance", [])

                cashflows_data.append(
                    CashflowModel(
                        **{
                            "profile_id": new_profile_id,
                            "index": index,

                            "age_householder": age_householder[index] if index < len(age_householder) else None,
                            "age_spouse": age_spouse[index] if index < len(age_spouse) else None,
                            "income_householder": income_householder[index] if index < len(income_householder) else None,
                            "pension_householder": pension_householder[index] if index < len(pension_householder) else None,
                            "income_spouse": income_spouse[index] if index < len(income_spouse) else None,
                            "pension_spouse": pension_spouse[index] if index < len(pension_spouse) else None,
                            "income_insurance": income_insurance[index] if index < len(income_insurance) else None,
                            "income_other": income_other[index] if index < len(income_other) else None,
                            "income_sum": income_sum[index] if index < len(income_sum) else None,
                            "expense_living": expense_living[index] if index < len(expense_living) else None,
                            "expense_insurance": expense_insurance[index] if index < len(expense_insurance) else None,
                            "expense_home": expense_home[index] if index < len(expense_home) else None,
                            "repayment_home": repayment_home[index] if index < len(repayment_home) else None,
                            "expense_child": expense_child[index] if index < len(expense_child) else None,
                            "expense_car": expense_car[index] if index < len(expense_car) else None,
                            "tax_and_social_ins": tax_and_social_ins[index] if index < len(tax_and_social_ins) else None,
                            "home_loan_deduction": home_loan_deduction[index] if index < len(home_loan_deduction) else None,
                            "repayment_other": repayment_other[index] if index < len(repayment_other) else None,
                            "expense_event": expense_event[index] if index < len(expense_event) else None,
                            "expense_sum": expense_sum[index] if index < len(expense_sum) else None,
                            "assets_liquidity": assets_liquidity[index] if index < len(assets_liquidity) else None,
                            "assets_under_mgmt": assets_under_mgmt[index] if index < len(assets_under_mgmt) else None,
                            "homeloans_balance": homeloans_balance[index] if index < len(homeloans_balance) else None,
                            "death_benefit": death_benefit[index] if index < len(death_benefit) else None,
                            "financial_asset_purchase": financial_asset_purchase[index] if index < len(financial_asset_purchase) else None,
                            "assets_ideco": assets_ideco[index] if index < len(assets_ideco) else None,
                            "assets_normal": assets_normal[index] if index < len(assets_normal) else None,
                            "assets_real_estate": assets_real_estate[index] if index < len(assets_real_estate) else None,
                            "assets_insurance": assets_insurance[index] if index < len(assets_insurance) else None,
                            "car_loan_balance": car_loan_balance[index] if index < len(car_loan_balance) else None,
                            "other_loan_balance": other_loan_balance[index] if index < len(other_loan_balance) else None,
                            "nisa_balance": nisa_balance[index] if index < len(nisa_balance) else None,



                            "asset_ammount_age_65": result.get("asset_ammount", {}).get(
                                "65"
                            ),
                            "asset_ammount_age_65": result.get("asset_ammount", {}).get(
                                "85"
                            ),
                            "created_at": result.get("created_at"),
                            "updated_at": result.get("updated_at"),
                        }
                    ).dict()
                )

                # 資産額推移

                financial_asset_chart_data = result.get("financial_asset_chart_data", [])
                living_capital = result.get("financial_asset_chart_data_adding", {}).get("living_capital", [])
                operating_asset_normal = result.get("financial_asset_chart_data_adding", {}).get("operating_asset_normal", [])
                nisa_balance = result.get("financial_asset_chart_data_adding", {}).get("nisa_balance", [])
                operating_asset_ideco = result.get("financial_asset_chart_data_adding", {}).get("operating_asset_ideco", [])
                assets_insurance = result.get("financial_asset_chart_data_adding", {}).get("assets_insurance", [])
                real_estate = result.get("financial_asset_chart_data_adding", {}).get("real_estate", [])
                home_loan = result.get("financial_asset_chart_data_adding", {}).get("home_loan", [])
                car_loan = result.get("financial_asset_chart_data_adding", {}).get("car_loan", [])
                card_loan = result.get("financial_asset_chart_data_adding", {}).get("card_loan", [])

                financial_asset_charts_data.append(
                    FinancialAssetChartModel(
                        **{
                            "profile_id": new_profile_id,

                            "age": financial_asset_chart_data[index][0] if index < len(financial_asset_chart_data) else None,
                            "asset": financial_asset_chart_data[index][1] if index < len(financial_asset_chart_data) else None,
                            "living_capital": living_capital[index][1] if index < len(living_capital) else None,
                            "operating_asset_normal": operating_asset_normal[index][1] if index < len(operating_asset_normal) else None,
                            "nisa_balance": nisa_balance[index][1] if index < len(nisa_balance) else None,
                            "operating_asset_ideco": operating_asset_ideco[index][1] if index < len(operating_asset_ideco) else None,
                            "assets_insurance": assets_insurance[index][1] if index < len(assets_insurance) else None,
                            "real_estate": real_estate[index][1] if index < len(real_estate) else None,
                            "home_loan": home_loan[index][1] if index < len(home_loan) else None,
                            "car_loan": car_loan[index][1] if index < len(car_loan) else None,
                            "card_loan": card_loan[index][1] if index < len(card_loan) else None,
                            
                            "created_at": result.get("created_at"),
                            "updated_at": result.get("updated_at"),
                        }
                    ).dict()
                )

            if cashflows_data:
                spark.createDataFrame(cashflows_data, CashflowStruct).write.mode(
                    "append"
                ).saveAsTable(f"{configs.DELTA_SCHEMA}.cashflows")
            if financial_asset_charts_data:
                spark.createDataFrame(financial_asset_charts_data, FinancialAssetChartStruct).write.mode(
                    "append"
                ).saveAsTable(f"{configs.DELTA_SCHEMA}.financial_asset_charts")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id,
                name="result_relevants_parse_task",
                error_message=str(e),
            ).dict()
        )
        raise e
