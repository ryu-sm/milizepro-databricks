# Databricks notebook source
# MAGIC %run ../common

# COMMAND ----------

if __name__ == "__main__":
    try:
        task_id = dbutils.widgets.get("task_id")
        update_task_status(id=task_id, status=ENUMS_TASK_STATUS.SUCCESS.value)
        update_sub_job_controller_status(
            status=ENUMS_SUB_JOB_CONTROLLER_STATUS.IDLE.value
        )
        task_id = dbutils.widgets.get("task_id")
        print(f"タスク[{task_id}]が実行します。")

    except Exception as e:
        update_task_status(task_id, status=ENUMS_TASK_STATUS.ERROR.value)
        insert_task_error_logs(
            TaskErrorLogModel(
                task_id=task_id, name="sub_job_end_task", error_message=str(e)
            ).dict()
        )
        raise e
