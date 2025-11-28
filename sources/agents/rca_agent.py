import platform, os
import asyncio
from sources.utility import pretty_print, animate_thinking
from sources.agents.agent import Agent
from sources.tools.rca_tools import ListTablesInDirectory, GetSchema, QueryParquetFiles
from sources.logger import Logger
from sources.memory import Memory

class RCAAgent(Agent):
    """
    The RCA agent is specialized for Root Cause Analysis using Parquet files.
    """
    def __init__(self, name, prompt_path, provider, verbose=False):
        super().__init__(name, prompt_path, provider, verbose, None)
        self.tools = {
            "list_tables_in_directory": ListTablesInDirectory(),
            "get_schema": GetSchema(),
            "query_parquet_files": QueryParquetFiles()
        }
        self.work_dir = self.tools["list_tables_in_directory"].get_work_dir()
        self.role = "rca"
        self.type = "rca_agent"
        self.logger = Logger("rca_agent.log")
        self.memory = Memory(self.load_prompt(prompt_path),
                        recover_last_session=False,
                        memory_compression=False,
                        model_provider=provider.get_model_name())
    
    def add_sys_info_prompt(self, prompt):
        """Add system information to the prompt."""
        target_dir = self.work_dir
        
        # Read problem.json
        problem_desc = ""
        problem_path = os.path.join(self.work_dir, "problem.json")
        if os.path.exists(problem_path):
            try:
                with open(problem_path, 'r') as f:
                    problem_desc = f.read()
            except Exception as e:
                problem_desc = f"Error reading problem.json: {e}"

        info = f"System Info:\n" \
               f"OS: {platform.system()} {platform.release()}\n" \
               f"Python Version: {platform.python_version()}\n" \
               f"\nCurrent Working Directory: {self.work_dir}\n" \
               f"Target Directory for Analysis: {target_dir}\n\n" \
               f"Problem Description:\n{problem_desc}"
        return f"{prompt}\n\n{info}"

    async def process(self, prompt, speech_module=None) -> str:
        answer = ""
        reasoning = ""
        attempt = 0
        max_attempts = 15
        prompt = self.add_sys_info_prompt(prompt)
        self.memory.push('user', prompt)
        
        while attempt < max_attempts and not self.stop:
            animate_thinking("Thinking...", color="status")
            await self.wait_message(speech_module)
            answer, reasoning = await self.llm_request()
            self.last_reasoning = reasoning
            
            if not "```" in answer:
                self.last_answer = answer
                await asyncio.sleep(0)
                break
            
            self.show_answer()
            animate_thinking("Executing tools...", color="status")
            self.status_message = "Executing tools..."
            self.logger.info(f"Attempt {attempt + 1}:\n{answer}")
            
            exec_success, feedback = self.execute_modules(answer)
            self.logger.info(f"Execution result: {exec_success}")
            
            answer = self.remove_blocks(answer)
            self.last_answer = answer
            await asyncio.sleep(0)
            
            if not exec_success:
                pretty_print(f"Execution failure:\n{feedback}", color="failure")
                pretty_print("Correcting...", color="status")
                self.status_message = "Correcting..."
            
            attempt += 1
            
        self.status_message = "Ready"
        if attempt == max_attempts:
            return "I'm sorry, I couldn't find the root cause within the limit.", reasoning
        
        return answer, reasoning
