# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")

        profile_orig_id = dbutils.widgets.get("profile_orig_id")

        new_profile_id = dbutils.widgets.get("new_profile_id")

        orig_profile = get_orig_profile(profile_orig_id)

        children_data = []
        child_schools_data = []
        child_tuitions_data = []
        child_events_data = []

        for child_idx, child in enumerate(orig_profile.get("子供", [])):
            user_childcare_leaves = (
                get_user_childcare_leaves(profile_orig_id, child_idx) or {}
            )
            child_data = ChildModel(
                **{
                    **child,
                    **user_childcare_leaves,
                    "profile_id": new_profile_id,
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )
            children_data.append(child_data.dict())

            # 幼稚園
            child_school_data = ChildSchoolModel(
                **{
                    "child_id": child_data.id,
                    "category": child.get("幼稚園"),
                    "prefectures": None
                    if child.get("幼稚園詳細", {}).get("0歳") == 0
                    else child.get("幼稚園詳細", {}).get("prefectures"),
                    "municipality": None
                    if child.get("幼稚園詳細", {}).get("0歳") == 0
                    else child.get("幼稚園詳細", {}).get("municipality"),
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )

            child_schools_data.append(child_school_data.dict())

            for grade_or_age in range(0, 7):
                if child.get("幼稚園詳細", {}).get(f"{grade_or_age}歳") is None:
                    continue
                child_tuition_data = ChildTuitionModel(
                    **{
                        "child_school_id": child_school_data.id,
                        "grade_or_age": grade_or_age,
                        "tuition": child.get("幼稚園詳細", {}).get(f"{grade_or_age}歳"),
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )

                child_tuitions_data.append(child_tuition_data.dict())

            # 小学校
            child_school_data = ChildSchoolModel(
                **{
                    "child_id": child_data.id,
                    "category": child.get("小学校"),
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )

            child_schools_data.append(child_school_data.dict())

            for grade_or_age in range(0, 7):
                if child.get("小学校詳細", {}).get(f"{grade_or_age}年") is None:
                    continue
                child_tuition_data = ChildTuitionModel(
                    **{
                        "child_school_id": child_school_data.id,
                        "grade_or_age": grade_or_age,
                        "tuition": child.get("小学校詳細", {}).get(f"{grade_or_age}年"),
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )

                child_tuitions_data.append(child_tuition_data.dict())

            # 中学校
            child_school_data = ChildSchoolModel(
                **{
                    "child_id": child_data.id,
                    "category": child.get("中学校")
                    if child.get("中学詳細", {}).get("入学金") == "0"
                    else child.get("中学詳細", {}).get("公立私立"),
                    "prefectures": None
                    if child.get("中学詳細", {}).get("学校名") == ""
                    else child.get("中学詳細", {}).get("prefectures"),
                    "name": None
                    if child.get("中学詳細", {}).get("学校名") == ""
                    else child.get("中学詳細", {}).get("学校名"),
                    "admission_fee": child.get("中学詳細", {}).get("入学金")
                    if child.get("中学詳細", {}).get("入学金") != "0"
                    else None,
                    "tuition_fee": child.get("中学詳細", {}).get("授業料")
                    if child.get("中学詳細", {}).get("授業料") != "0"
                    else None,
                    "additional_fee": child.get("中学詳細", {}).get("諸経費")
                    if child.get("中学詳細", {}).get("諸経費") != "0"
                    else None,
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )

            child_schools_data.append(child_school_data.dict())

            for grade_or_age in range(0, 7):
                if child.get("中学詳細", {}).get(f"{grade_or_age}年") is None:
                    continue
                child_tuition_data = ChildTuitionModel(
                    **{
                        "child_school_id": child_school_data.id,
                        "grade_or_age": grade_or_age,
                        "tuition": child.get("中学詳細", {}).get(f"{grade_or_age}年"),
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )

                child_tuitions_data.append(child_tuition_data.dict())

            # 高校
            child_school_data = ChildSchoolModel(
                **{
                    "child_id": child_data.id,
                    "category": child.get("高校")
                    if child.get("高校詳細", {}).get("入学金") == "0"
                    else child.get("高校詳細", {}).get("公立私立"),
                    "prefectures": None
                    if child.get("高校詳細", {}).get("学校名") == ""
                    else child.get("高校詳細", {}).get("prefectures"),
                    "name": None
                    if child.get("高校詳細", {}).get("学校名") == ""
                    else child.get("高校詳細", {}).get("学校名"),
                    "admission_fee": child.get("高校詳細", {}).get("入学金")
                    if child.get("高校詳細", {}).get("入学金") != "0"
                    else None,
                    "tuition_fee": child.get("高校詳細", {}).get("授業料")
                    if child.get("高校詳細", {}).get("授業料") != "0"
                    else None,
                    "additional_fee": child.get("高校詳細", {}).get("諸経費")
                    if child.get("高校詳細", {}).get("諸経費") != "0"
                    else None,
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )

            child_schools_data.append(child_school_data.dict())

            for grade_or_age in range(0, 7):
                if child.get("高校詳細", {}).get(f"{grade_or_age}年") is None:
                    continue
                child_tuition_data = ChildTuitionModel(
                    **{
                        "child_school_id": child_school_data.id,
                        "grade_or_age": grade_or_age,
                        "tuition": child.get("高校詳細", {}).get(f"{grade_or_age}年"),
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )

                child_tuitions_data.append(child_tuition_data.dict())

            # 大学
            child_school_data = ChildSchoolModel(
                **{
                    "child_id": child_data.id,
                    "category": child.get("大学")
                    if child.get("大学詳細", {}).get("入学金") == "0"
                    else child.get("大学詳細", {}).get("公立私立"),
                    "prefectures": None
                    if child.get("大学詳細", {}).get("学校名") == ""
                    else child.get("大学詳細", {}).get("prefectures"),
                    "name": None
                    if child.get("大学詳細", {}).get("学校名") == ""
                    else child.get("大学詳細", {}).get("学校名"),
                    "admission_fee": child.get("大学詳細", {}).get("入学金")
                    if child.get("大学詳細", {}).get("入学金") != "0"
                    else None,
                    "tuition_fee": child.get("大学詳細", {}).get("授業料")
                    if child.get("大学詳細", {}).get("授業料") != "0"
                    else None,
                    "additional_fee": child.get("大学詳細", {}).get("諸経費")
                    if child.get("大学詳細", {}).get("諸経費") != "0"
                    else None,
                    "commuting_style": child.get("通学形態"),
                    "created_at": orig_profile.get("created_at"),
                    "updated_at": orig_profile.get("updated_at"),
                }
            )

            child_schools_data.append(child_school_data.dict())

            for grade_or_age in range(0, 7):
                if child.get("大学詳細", {}).get(f"{grade_or_age}年") is None:
                    continue
                child_tuition_data = ChildTuitionModel(
                    **{
                        "child_school_id": child_school_data.id,
                        "grade_or_age": grade_or_age,
                        "tuition": child.get("大学詳細", {}).get(f"{grade_or_age}年"),
                        "created_at": orig_profile.get("created_at"),
                        "updated_at": orig_profile.get("updated_at"),
                    }
                )

                child_tuitions_data.append(child_tuition_data.dict())

            # その他学校
            other_schools = (
                [child.get("その他学校", {})]
                if type(child.get("その他学校") == dict)
                else child.get("その他学校", [])
            )
            for other_school in other_schools:
                child_schools_data.append(
                    ChildSchoolModel(
                        **{
                            "child_id": child_data.id,
                            "category": "その他学校",
                            "name": other_school.get("学校名"),
                            "start_age": other_school.get("開始年齢"),
                            "end_age": other_school.get("修了年齢"),
                            "admission_fee": other_school.get("入学金"),
                            "tuition_fee": other_school.get("授業料"),
                            "created_at": orig_profile.get("created_at"),
                            "updated_at": orig_profile.get("updated_at"),
                        }
                    ).dict()
                )

            # 留学
            study_abroads = (
                [child.get("留学", {})]
                if type(child.get("留学") == dict)
                else child.get("留学", [])
            )
            for study_abroad in study_abroads:
                child_schools_data.append(
                    ChildSchoolModel(
                        **{
                            "child_id": child_data.id,
                            "category": "留学",
                            "start_age": study_abroad.get("開始年齢"),
                            "end_age": int(study_abroad.get("開始年齢"))
                            + int(study_abroad.get("期間"))
                            if study_abroad.get("期間") and study_abroad.get("開始年齢")
                            else None,
                            "tuition_fee": study_abroad.get("費用"),
                            "created_at": orig_profile.get("created_at"),
                            "updated_at": orig_profile.get("updated_at"),
                        }
                    ).dict()
                )

            # イベント
            for child_event in child.get("イベント", []):
                child_events_data.append(
                    ChildEventModel(
                        **{
                            **child_event,
                            "child_id": child_data.id,
                            "created_at": orig_profile.get("created_at"),
                            "updated_at": orig_profile.get("updated_at"),
                        }
                    ).dict()
                )

        if children_data:
            spark.createDataFrame(children_data, ChildStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.children")
        if child_schools_data:
            spark.createDataFrame(child_schools_data, ChildSchoolStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.child_schools")
        if child_tuitions_data:
            spark.createDataFrame(child_tuitions_data, ChildTuitionStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.child_tuitions")
        if child_events_data:
            spark.createDataFrame(child_events_data, ChildEventStruct).write.mode(
                "append"
            ).saveAsTable(f"{configs.DELTA_SCHEMA}.child_events")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="child_relevants", error_message=str(e)
            ).dict()
        )
        raise e
