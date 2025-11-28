# 项目修改记录

## 1. 新增自动化分析脚本
- **创建 `auto_analyze.py`**: 
  - 用于自动化执行 RCA (Root Cause Analysis) 任务。
  - 读取 `question_3/problem.json` 作为输入。
  - 初始化 `RCAAgent` 并直接执行分析，绕过 Planner 的复杂调度。
  - 将最终结果和完整对话历史保存到 `output.json`。

## 2. 工具层修改 (`sources/tools/rca_tools.py`)jy7
- **新增 RCA 专用工具**:
  - `ListTablesInDirectory`: 列出指定目录下的 Parquet 表。
  - `GetSchema`: 获取 Parquet 文件的 Schema 信息。
  - `QueryParquetFiles`: 使用 DuckDB 执行 SQL 查询分析 Parquet 文件。
- **路径处理优化**:
  - 在工具实现中增加了对相对路径的支持，如果当前工作目录找不到文件，会自动尝试在 `work_dir` 下查找，解决了 "File not found" 的问题。

## 3. Agent 配置修改 (`sources/agents/rca_agent.py`)
- **工具列表精简**:
  - 移除了 `Bash` 和 `FileFinder` 等通用工具，强制 Agent 仅使用上述三个 SQL 分析工具，防止 Agent 尝试使用系统命令。
- **上下文注入**:
  - 修改 `add_sys_info_prompt` 方法，使其能够读取并注入 `problem.json` 中的问题描述和背景信息。

## 4. Prompt 优化 (`prompts/base/`)
- **`rca_agent.txt`**:
  - 更新了系统提示词，明确了 "Read -> Explore -> Understand -> Analyze" 的标准工作流。
  - 强调了必须使用 SQL 工具进行数据分析。
- **`planner_agent.txt`**:
  - 增加了约束条件，强制 Planner 将所有任务仅分配给 `RCA` Agent，防止幻觉产生不存在的 Agent (如 `File`, `Code`)。

## 5. 底层配置修改 (`sources/llm_provider.py`)
- **超时时间调整**:
  - 将 OpenAI API 的请求超时时间从默认值增加到 120 秒，以应对复杂的 SQL 生成和分析任务可能导致的响应延迟。

## 6. Bug 修复
- **`auto_analyze.py` 日志保存**:
  - 修复了 `output.json` 保存失败的问题。修正了代码中获取对话历史的方式，从 `interaction.conversation_history` (不存在的属性) 修改为 `interaction.current_agent.memory.get()`。
