# IMPORTANT IMPORT ORDER (unsloth finetuning)
# =============================================================================

import os
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import gc
import torch

# IMPORTANT: import unsloth before trl / transformers / peft
from unsloth import FastLanguageModel
from unsloth.chat_templates import get_chat_template

from datasets import load_dataset
from trl import SFTTrainer, SFTConfig


# =============================================================================
# CONFIG
# =============================================================================
TRAIN_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-llama-placeholder/llama_esg_placeholder_train_24-25 (1).jsonl"
VALID_JSONL = "/kaggle/input/datasets/vaibhavmeena23/final-llama-placeholder/llama_esg_placeholder_valid_24-25 (1).jsonl"

OUTPUT_DIR = "/kaggle/working/llama_esg_placeholder_lora_v2"

MODEL_NAME = "unsloth/Meta-Llama-3.1-8B-Instruct-bnb-4bit"

MAX_SEQ_LENGTH = 4096

USE_SMALL_SUBSET = False
MAX_TRAIN_EXAMPLES = None
MAX_VALID_EXAMPLES = None

MAX_STEPS = 500


# =============================================================================
# GPU CHECK
# =============================================================================

print("CUDA available:", torch.cuda.is_available())

if not torch.cuda.is_available():
    raise RuntimeError("No GPU found. Enable GPU accelerator.")

print("GPU:", torch.cuda.get_device_name(0))
print("VRAM GB:", round(torch.cuda.get_device_properties(0).total_memory / 1024**3, 2))

torch.cuda.empty_cache()
gc.collect()


# =============================================================================
# LOAD MODEL
# =============================================================================

model, tokenizer = FastLanguageModel.from_pretrained(
    model_name = MODEL_NAME,
    max_seq_length = MAX_SEQ_LENGTH,
    dtype = None,
    load_in_4bit = True,
)

tokenizer = get_chat_template(
    tokenizer,
    chat_template = "llama-3.1",
)

model = FastLanguageModel.get_peft_model(
    model,
    r = 16,
    target_modules = [
        "q_proj", "k_proj", "v_proj", "o_proj",
        "gate_proj", "up_proj", "down_proj",
    ],
    lora_alpha = 16,
    lora_dropout = 0,
    bias = "none",
    use_gradient_checkpointing = "unsloth",
    random_state = 42,
)


# =============================================================================
# LOAD DATASET
# =============================================================================

dataset = load_dataset(
    "json",
    data_files={
        "train": TRAIN_JSONL,
        "validation": VALID_JSONL,
    }
)

print("Raw train rows:", len(dataset["train"]))
print("Raw valid rows:", len(dataset["validation"]))
print("Raw columns:", dataset["train"].column_names)

# IMPORTANT: placeholder dataset task name
dataset = dataset.filter(
    lambda x: x.get("task") == "section_generation_placeholder"
)

print("After task filter train rows:", len(dataset["train"]))
print("After task filter valid rows:", len(dataset["validation"]))

# Optional: keep all placeholder target types
ALLOWED_TARGET_SOURCES = {
    "placeholder_kpi_grounded_target",
    "placeholder_missing_data",
    "placeholder_disclosure_limitations",
}

dataset = dataset.filter(
    lambda x: x.get("target_source") in ALLOWED_TARGET_SOURCES
)

print("After target_source filter train rows:", len(dataset["train"]))
print("After target_source filter valid rows:", len(dataset["validation"]))

if len(dataset["train"]) == 0:
    raise ValueError(
        "Train dataset is empty. Check that TRAIN_JSONL path is correct and filters match placeholder dataset labels."
    )

# =============================================================================
# FORMAT MESSAGES TO TEXT
# =============================================================================

def formatting_prompts_func(examples):
    texts = []

    for messages in examples["messages"]:
        text = tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=False,
        )
        texts.append(text)

    return {"text": texts}


dataset = dataset.map(
    formatting_prompts_func,
    batched=True,
    remove_columns=[],
)

# Remove empty text rows just in case
dataset = dataset.filter(
    lambda x: isinstance(x.get("text"), str) and len(x["text"].strip()) > 0
)

print("Final train rows:", len(dataset["train"]))
print("Final valid rows:", len(dataset["validation"]))
print("Columns:", dataset["train"].column_names)

if len(dataset["train"]) == 0:
    raise ValueError("Final train dataset is empty after formatting.")

print("Sample text chars:", len(dataset["train"][0]["text"]))
print(dataset["train"][0]["text"][:1500])


# =============================================================================
# SFT CONFIG
# =============================================================================

training_args = SFTConfig(
    output_dir = OUTPUT_DIR,

    dataset_text_field = "text",
    max_length = MAX_SEQ_LENGTH,
    packing = False,
    padding_free = False,   # IMPORTANT FIX

    per_device_train_batch_size = 1,
    gradient_accumulation_steps = 8,

    max_steps = MAX_STEPS,

    learning_rate = 1e-4,
    warmup_steps = 15,
    weight_decay = 0.01,
    lr_scheduler_type = "cosine",

    fp16 = True,
    bf16 = False,

    logging_steps = 10,

    eval_strategy = "no",

    save_strategy = "steps",
    save_steps = 100,
    save_total_limit = 3,

    optim = "adamw_8bit",
    seed = 42,
    report_to = "none",
)


# =============================================================================
# TRAINER
# =============================================================================

trainer = SFTTrainer(
    model = model,
    args = training_args,
    processing_class = tokenizer,
    train_dataset = dataset["train"],
    eval_dataset = None,
)

trainer.train()


# =============================================================================
# SAVE LORA
# =============================================================================

FINAL_LORA_DIR = f"{OUTPUT_DIR}/final_lora"

model.save_pretrained(FINAL_LORA_DIR)
tokenizer.save_pretrained(FINAL_LORA_DIR)

print("Saved LoRA:", FINAL_LORA_DIR)