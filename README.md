# AI Weekly Report

当前自动触发时间：北京时间每周一上午 `09:00`。

配置文件：

- `.github/workflows/weekly-report.yml`

当前配置：

```yaml
schedule:
  - cron: "0 1 * * 1"
  代表：分钟 小时（utc） * * 每周几 
```

GitHub Actions 使用 `UTC` 时间，北京时间 = `UTC+8`。修改触发时间时，直接修改这个 `cron` 即可。

如果自动流失效，下载代码后可手动生成网页报告：

```bash
python generate_web_report.py --input workflow_runs.jsonl --output workflow_report.html
```
