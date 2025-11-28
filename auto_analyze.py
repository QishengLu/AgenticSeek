#!/usr/bin/env python3

import sys
import argparse
import configparser
import asyncio
import warnings
import json
import os

from sources.llm_provider import Provider
from sources.interaction import Interaction
from sources.agents import RCAAgent
from sources.utility import pretty_print

warnings.filterwarnings("ignore")

config = configparser.ConfigParser()
config.read('config.ini')

async def main():
    pretty_print("Initializing Auto Analysis...", color="status")
    
    # Setup Provider
    provider = Provider(provider_name=config["MAIN"]["provider_name"],
                        model=config["MAIN"]["provider_model"],
                        server_address=config["MAIN"]["provider_server_address"],
                        is_local=config.getboolean('MAIN', 'is_local'))

    # Setup RCA Agent
    # We only need the RCA Agent for this task
    rca_agent = RCAAgent(name="RCA Agent",
                         prompt_path="prompts/base/rca_agent.txt",
                         provider=provider, 
                         verbose=True) # Set verbose to True to see what's happening

    agents = [rca_agent]

    # Setup Interaction
    interaction = Interaction(agents,
                              tts_enabled=False,
                              stt_enabled=False,
                              recover_last_session=False,
                              langs=["en"]
                            )
    
    # Set the specific query for RCA
    problem_path = os.path.join("question_3", "problem.json")
    if os.path.exists(problem_path):
        with open(problem_path, "r") as f:
            problem_content = f.read()
        query = f"{problem_content}\n\nPlease follow the instructions above to analyze the parquet files in the current directory."
    else:
        query = "Follow the workflow in the Problem Description to analyze the parquet files in the current directory."
    
    interaction.set_query(query)
    
    pretty_print(f"Starting analysis...", color="info")
    
    try:
        # Run the agent
        if await interaction.think():
            print("\n" + "="*50)
            print("FINAL ANSWER:")
            print("="*50)
            print(interaction.last_answer)
            print("="*50)
        else:
            print("Agent failed to produce an answer.")
        
        # Get conversation history from agent's memory
        agent = interaction.current_agent
        conversation_history = []
        if agent and hasattr(agent, 'memory') and agent.memory:
            conversation_history = agent.memory.get()
        
        # Save conversation history to output.json
        output_data = {
            "query": query[:500] + "..." if len(query) > 500 else query,  # Truncate query for readability
            "conversation_history": conversation_history,
            "final_answer": interaction.last_answer if hasattr(interaction, 'last_answer') else None,
            "final_reasoning": interaction.last_reasoning if hasattr(interaction, 'last_reasoning') else None
        }
        
        output_path = os.path.join(os.getcwd(), "output.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        pretty_print(f"✓ Output saved to {output_path}", color="success")
        pretty_print(f"✓ Problem description loaded and processed", color="success")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e

if __name__ == "__main__":
    asyncio.run(main())
