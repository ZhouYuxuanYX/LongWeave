#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import string
import argparse
import math
import re
import io
import copy # For deep copying scopes/configs
import sys # For exit on error
import traceback # For debugging generator errors
import datetime # For B008 violation

# --- Configuration ---
TARGET_LINE_LENGTH = 79
TARGET_COMMENT_LENGTH = 72
MAX_FUNC_LINES = 40 # Slightly increased
MAX_BLOCK_LINES = 30 # Slightly increased
MAX_NESTING = 4 # Slightly increased

# --- Word Lists (Defined globally and early) ---
NOUNS = ['customer', 'record', 'data', 'age', 'email', 'order', 'value', 'report', 'summary', 'list', 'result', 'id', 'status', 'batch', 'log', 'file', 'config', 'item', 'user', 'product', 'system', 'input', 'output', 'state', 'context', 'cache', 'queue', 'stream', 'event', 'message'] # Added more generic nouns
VERBS = ['load', 'validate', 'process', 'check', 'calculate', 'generate', 'get', 'set', 'update', 'filter', 'clean', 'analyze', 'save', 'print', 'handle', 'find', 'parse', 'send', 'verify', 'fetch', 'receive', 'transform', 'aggregate', 'dispatch', 'resolve'] # Added more verbs
ADJECTIVES = ['valid', 'invalid', 'clean', 'raw', 'average', 'total', 'active', 'inactive', 'new', 'main', 'temp', 'global', 'final', 'pending', 'primary', 'secondary', 'cached', 'processed', 'aggregated', 'current', 'previous', 'next'] # Added more adjectives
KEYWORDS = {"if", "else", "for", "while", "def", "class", "import", "from", "return", "yield", "try", "except", "finally", "with", "as", "pass", "break", "continue", "in", "is", "not", "and", "or", "lambda", "None", "True", "False", "global", "nonlocal", "assert", "async", "await", "del"}

# --- Global State for Harder Violations ---
GLOBAL_COUNTER = random.randint(0, 10) # For global modification example
GLOBAL_FLAG = random.choice([True, False]) # Another global to modify

# --- Scope Management ---
class Scope:
    def __init__(self, parent=None):
        self.parent = parent
        self.variables = {} # name -> {type_hint: str, defined_line: int, used: bool}

    def define(self, name, type_hint='any', line_num=0):
        # Basic validation
        if not name or not isinstance(name, str) or not name.isidentifier() or name in KEYWORDS:
             # print(f"DEBUG: Define failed basic validation for '{name}'", file=sys.stderr)
             return False
        # Check if defined LOCALLY (shadowing is allowed but discouraged by linters)
        if name in self.variables:
            # print(f"DEBUG: Define failed, '{name}' already defined locally", file=sys.stderr)
            return False
        self.variables[name] = {'type': type_hint, 'line': line_num, 'used': False}
        # print(f"DEBUG: Defined '{name}' (type: {type_hint}) in scope {id(self)}", file=sys.stderr)
        return True

    def mark_used(self, name):
        if not name or not isinstance(name, str): return False
        target_scope = self._find_scope_with_var(name)
        if target_scope:
            target_scope.variables[name]['used'] = True
            # print(f"DEBUG: Marked '{name}' as used in scope {id(target_scope)}", file=sys.stderr)
            return True
        # print(f"DEBUG: Mark used failed, '{name}' not found", file=sys.stderr)
        return False

    def _find_scope_with_var(self, name):
        """Helper to find the scope where a variable is defined."""
        if name in self.variables:
            return self
        elif self.parent:
            return self.parent._find_scope_with_var(name)
        return None

    def is_defined(self, name, check_parents=True):
        if not name or not isinstance(name, str): return False
        if name in self.variables: return True
        if check_parents and self.parent:
            return self.parent.is_defined(name, check_parents=True)
        return False

    def is_defined_locally(self, name):
        if not name or not isinstance(name, str): return False
        return name in self.variables

    def get_all_defined_vars(self, type_filter=None, used_status=None):
        names = set()
        current = self
        while current:
            current_level_vars = {
                name for name, info in current.variables.items()
                if (type_filter is None or info.get('type') == type_filter) and \
                   (used_status is None or info.get('used') == used_status)
            }
            # Filter out non-identifiers just in case
            names.update({n for n in current_level_vars if isinstance(n, str) and n.isidentifier()})
            current = current.parent
        return list(names)

    def get_random_defined_var(self, type_filter=None, used_status=None):
         candidates = self.get_all_defined_vars(type_filter=type_filter, used_status=used_status)
         if not candidates: return None
         chosen_var = random.choice(candidates)
         # Mark used in the correct scope
         self.mark_used(chosen_var)
         return chosen_var

    def get_var_type(self, name):
        """Gets the type hint of a variable, searching parents."""
        target_scope = self._find_scope_with_var(name)
        if target_scope:
            return target_scope.variables[name].get('type', 'any')
        return None # Not defined

    def get_all_vars_with_info(self):
         all_vars = {}
         if self.parent: all_vars.update(self.parent.get_all_vars_with_info())
         all_vars.update(self.variables)
         return all_vars

    # MODIFIED: Helper to check parent scopes for shadowing potential (informational)
    def check_shadowing(self, name):
        if self.parent and self.parent.is_defined(name, check_parents=True):
            return True
        return False

# --- Basic Helpers (Defined early) ---
def should_violate(probability):
    # Slightly increase base chance for more violations overall
    effective_prob = min(1.0, max(0.0, probability + 0.1))
    return random.random() < effective_prob

def gen_indent_spaces(level, probability=0):
    # Add small chance of inconsistent indent (W191) - CAUTION: breaks runnability if mixed tabs/spaces
    # if should_violate(probability * 0.05):
    #     return (" " * (level * 4 - random.randint(1,2))) + "\t" # Risky!
    return " " * (level * 4)

def _contains_executable(lines):
    return any(l.strip() and not l.lstrip().startswith('#') for l in lines)

# --- Naming and Value Generators (Defined early) ---
def gen_meaningful_name(parts=2):
    name_parts = []
    # Higher chance of adjective prefix
    if ADJECTIVES and random.random() < 0.7 and parts > 1: name_parts.append(random.choice(ADJECTIVES))
    verb_or_noun_added = False
    # Mix verbs and nouns more freely
    if VERBS and (not name_parts or random.random() < 0.6): name_parts.append(random.choice(VERBS)); verb_or_noun_added = True
    if NOUNS and (not verb_or_noun_added or random.random() < 0.6): name_parts.append(random.choice(NOUNS)); verb_or_noun_added = True
    # Add more nouns, potentially generic ones
    while len(name_parts) < parts and NOUNS: name_parts.append(random.choice(NOUNS))
    if not name_parts: name_parts.append(f"var{random.randint(100,999)}")
    # Higher chance of number suffix
    if random.random() < 0.2: name_parts.append(str(random.randint(1, 99))) # Increased range
    res = "_".join(name_parts[:parts+1]) # Allow slightly longer names
    if not res or (res[0].isdigit()) or (not res[0].isalpha() and res[0] != '_'): res = 'g_' + res
    res = re.sub(r'[^a-zA-Z0-9_]', '_', res)
    if res.endswith('_'): res = res[:-1] + 'x'
    if not res: res = "fallback_name"
    if res in KEYWORDS: res += "_"
    return res

# MODIFIED: Generate harder-to-fix naming issues
def gen_variable_name(scope, name_type, base_name=None, probability=0.0): # N codes
    if base_name is None:
        try: base_name = gen_meaningful_name(random.randint(1, 3)) # Allow longer base names
        except Exception as e: base_name = f"fallback_base_{random.randint(100,999)}"

    name = base_name
    correct_style = "snake_case"
    if name_type in ["class", "exception"]: correct_style = "CapWords"
    if name_type == "constant": correct_style = "UPPER_SNAKE_CASE"

    # --- Introduce Naming Violations ---
    violation_applied = False
    # 1. Inconsistent Style (Higher probability)
    if should_violate(probability * 1.5): # Increased probability
        violation_applied = True
        if correct_style == "snake_case" and random.random() < 0.6: # Higher chance for this common error
             name = "".join(word.capitalize() for word in base_name.split("_") if word) or "BadCamelName"
             # Add chance of mixedCase
             if random.random() < 0.3: name = name[0].lower() + name[1:] if len(name)>1 else name.lower()
        elif correct_style == "CapWords":
             s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', base_name); name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower() or "bad_snake_name"
        elif correct_style == "UPPER_SNAKE_CASE":
             name = base_name.lower()
        # Add chance of single letter var name (N806) - avoid if base is long
        elif len(base_name) < 5 and random.random() < 0.2:
             name = random.choice(string.ascii_lowercase)

    # 2. Potentially Misleading Name (Subtle) - relies on gen_meaningful_name using generic words
    # (No direct code here, but the word list changes help)

    # 3. Shadowing Potential (Informational - actual shadowing depends on usage)
    # We don't force shadowing, but we check if the generated name *could* shadow
    shadows_parent = scope.check_shadowing(name)
    # if shadows_parent: print(f"DEBUG: Potential shadowing for '{name}'", file=sys.stderr)

    # --- Validation Loop (Mostly unchanged, ensures runnability) ---
    original_name = name
    attempts = 0
    while attempts < 15:
        is_keyword = name in KEYWORDS
        is_valid_id = name.isidentifier()
        is_defined_here = scope.is_defined_locally(name)

        if is_valid_id and not is_keyword and not is_defined_here:
            break # Found a valid, non-keyword, non-duplicate name for this scope

        # Modify name for next attempt
        if not is_valid_id or is_keyword:
            name += "_"
        elif is_defined_here:
            name = f"{original_name}_{attempts+1}"

        attempts += 1
        if attempts == 10: # Try a completely different base after 10 tries
             base_name = gen_meaningful_name(random.randint(1, 2))
             original_name = base_name
             name = base_name

    # Final fallback
    if attempts == 15 or not name.isidentifier() or name in KEYWORDS or scope.is_defined_locally(name):
        name = f"{name_type}_fallback_{random.randint(1000,9999)}"
        while not name.isidentifier() or name in KEYWORDS or scope.is_defined_locally(name):
            name += "_"

    return name


def gen_simple_value(var_type='any'):
    # Ensure generated value matches var_type more reliably
    if var_type == 'int': return str(random.randint(-50, 150)) # Wider range
    if var_type == 'str':
        # More varied strings
        return random.choice([
            f"'{random.choice(NOUNS)}_{random.choice(['id','ref','code','key'])}'",
            f"\"Status: {random.choice(ADJECTIVES).upper()}\"",
            f"'{''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5,15)))}'" # Random string
        ])
    if var_type == 'bool': return random.choice(['True', 'False'])
    if var_type == 'float': return str(round(random.uniform(-100.0, 200.0), random.randint(1,4))) # Variable precision
    if var_type == 'list': return f"[{random.randint(1,5)}, '{random.choice(NOUNS)}', {random.choice(['True', 'False'])}, {gen_simple_value('int')}]" # Slightly more complex
    if var_type == 'dict': return f"{{'{random.choice(NOUNS)}': {random.randint(1,10)}, 'status': {random.choice(['True', 'False'])}, 'nested': {{'a': 1}} }}" # Add nesting
    if var_type == 'None': return 'None'
    # Fallback for 'any' or unrecognized
    return random.choice([
        str(random.randint(0, 100)),
        f"'{random.choice(NOUNS)}'",
        'True', 'False', 'None',
        str(round(random.uniform(10.0, 100.0), 2))
    ])

# --- E/W Violation Generators (Mostly unchanged, focus on harder logic) ---
def gen_whitespace_around_op(op, probability):
    if should_violate(probability * 0.5): return op.strip() # E225
    elif should_violate(probability * 0.5): return f"{' ' * random.randint(2, 3)}{op.strip()}{' ' * random.randint(2, 3)}" # E221/E222
    else: return f" {op.strip()} "
def gen_whitespace_after_comma(probability):
    if should_violate(probability * 1.2): return "," # E231
    else: return ", "
def generate_blank_line(config, force_violation=False):
    violation_prob = config.get('violation_probability', 0.0)
    if force_violation or should_violate(violation_prob * 0.4): return " " * random.randint(1, 8) # W293
    else: return ""
def generate_long_line(indent_str, config): # E501
    scope = config.get('current_scope')
    line_num = config.get('current_line_num', 0)
    prob = config.get('violation_probability', 0.0)
    var_name = "long_variable_fallback"
    if scope:
        # Use harder name generator
        var_name = gen_variable_name(scope, "variable", f"long_variable_name_{random.randint(100,999)}", prob * 1.2)
        if not scope.define(var_name, 'str', line_num):
             return [], line_num
        scope.mark_used(var_name)
    else:
        return [], line_num

    base = f"{indent_str}{var_name} = "
    extra_chars = TARGET_LINE_LENGTH - len(base) + random.randint(25, 60) # Make lines longer
    # Make content slightly more complex
    value = "'" + " ".join([random.choice(NOUNS) for _ in range(extra_chars // 6)]) + "'"
    value = value[:max(1, extra_chars)] + "'" # Ensure length
    return [base + value], line_num + 1

def generate_trailing_whitespace(line): return line + (" " * random.randint(1, 5)) # W291
def generate_bad_comment_space(config): # E26x
     line_num = config.get('current_line_num', 0)
     indent_level = config.get('indent_level', 0)
     violation_prob = config.get('violation_probability', 0.0)
     indent_str = gen_indent_spaces(indent_level, 0)
     text = f"Comment text {random.randint(100,999)}"
     line = ""
     # Increase chance of E265/E266
     if should_violate(violation_prob * 0.6): line = f"{indent_str}## {text}" # E265/E266 (space after ## is common)
     elif should_violate(violation_prob * 0.4): line = f"{indent_str}#{text}" # E262
     else: line = f"{indent_str}# {text}"
     if not isinstance(line_num, int): line_num = 0
     return [line], line_num + 1

# --- List of Dicts Generator ---
def gen_list_of_dicts(num_records, config):
    violation_prob = config['violation_probability']
    records = []
    keys = [gen_meaningful_name(1) for _ in range(random.randint(2,4))] # More varied keys
    for i in range(num_records):
        rec = {k: gen_simple_value(random.choice(['int', 'str', 'bool'])) for k in keys}
        rec['id'] = f"'ID_{100+i}'" # Ensure ID exists
        records.append(rec)

    base_indent_level = config.get('indent_level', 0)
    indent1_str = gen_indent_spaces(base_indent_level + 1, 0)
    indent2_str = gen_indent_spaces(base_indent_level + 2, 0)
    outer_indent_str = gen_indent_spaces(base_indent_level, 0)
    list_str = "[\n"
    for i, rec in enumerate(records):
        list_str += f"{indent1_str}{{\n"
        items = []
        # Randomize key order slightly
        shuffled_keys = list(rec.keys())
        random.shuffle(shuffled_keys)
        for k in shuffled_keys:
            v = rec[k]
            colon = gen_whitespace_around_op(":", violation_prob * 0.8) # More whitespace issues
            # Ensure keys are strings
            items.append(f"{indent2_str}'{str(k)}'{colon}{v}")
        # Add trailing comma violation (E203 before colon handled by gen_whitespace_around_op)
        comma = ",\n" if i < num_records -1 or should_violate(violation_prob * 0.3) else "\n"
        list_str += (",\n").join(items) + f"\n{indent1_str}}}"
        if i < num_records - 1: list_str += ","
        # Add chance of missing newline after comma
        list_str += "\n" if random.random() > violation_prob * 0.2 else " "

    list_str += f"{outer_indent_str}]"
    return list_str

# --- Specific Violation Generators ---
def generate_unused_import(config): # F401
    indent_str = gen_indent_spaces(config['indent_level'], 0)
    # Use less common modules sometimes
    mod = random.choice(['os', 'datetime', 'itertools', 'functools', 'pathlib', 'json', 'csv', 'urllib.request', 'collections', 'heapq', 'socket', 'subprocess', 'tempfile', 'uuid'])
    return [f"{indent_str}import {mod}"], config.get('current_line_num', 0) + 1

def generate_unused_local_variable(current_scope, line_num, config): # F841
    indent_str = gen_indent_spaces(config['indent_level'], 0)
    violation_prob = config['violation_probability']
    # Use harder name generator
    var_name = gen_variable_name(current_scope, "variable", f"unused_{random.choice(NOUNS)}", violation_prob * 1.2)
    var_type = random.choice(['int', 'str', 'bool', 'float', 'list', 'dict', 'None'])
    value = gen_simple_value(var_type)

    defined = current_scope.define(var_name, var_type, line_num)
    if not defined: return [], line_num
    op_str = gen_whitespace_around_op("=", violation_prob)
    line = f"{indent_str}{var_name}{op_str}{value}"
    # NOTE: Intentionally not marked used
    return [line], line_num + 1

# --- Helper for Mutable/Function Call Param ---
# MODIFIED: Add B008 possibility
def _add_problematic_param_if_needed(params_list, func_scope, line_num, config, used_in_signature):
    violation_prob = config['violation_probability']
    param_added = False

    # B006: Mutable Default Argument
    if not param_added and should_violate(violation_prob * 0.5): # Increased chance
        param_base_part = random.choice(['cache', 'items', 'settings', 'context', 'state', 'buffer', 'log_entries'])
        param_base = f"mutable_{param_base_part}"
        param_name = None
        attempts = 0
        while attempts < 5: # Fewer attempts to increase chance of fallback/collision
            potential_name = gen_variable_name( func_scope, "parameter", param_base, violation_prob*0.5 )
            if potential_name not in used_in_signature:
                 # Define with 'any' or list/dict type hint
                 hint = random.choice(['list', 'dict', 'any'])
                 if func_scope.define(potential_name, hint, line_num):
                     param_name = potential_name
                     used_in_signature.add(param_name)
                     break
                 else: param_base += str(random.randint(0,9))
            else: param_base += str(random.randint(0,9))
            attempts += 1

        if param_name:
            mutable_default = random.choice(['[]', '{}'])
            param_str = f"{param_name}={mutable_default}"
            if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
            params_list.append(param_str)
            param_added = True
            # print(f"DEBUG: Added B006 param: {param_str}", file=sys.stderr)

    # B008: Function Call in Default Argument
    if not param_added and should_violate(violation_prob * 0.4): # Separate chance
        param_base_part = random.choice(['timestamp', 'request_id', 'default_config', 'creation_date'])
        param_base = f"dynamic_{param_base_part}"
        param_name = None
        attempts = 0
        while attempts < 5:
            potential_name = gen_variable_name( func_scope, "parameter", param_base, violation_prob*0.5 )
            if potential_name not in used_in_signature:
                 # Define with appropriate type hint if possible
                 hint = 'datetime' if 'time' in param_base or 'date' in param_base else 'any'
                 if func_scope.define(potential_name, hint, line_num):
                     param_name = potential_name
                     used_in_signature.add(param_name)
                     break
                 else: param_base += str(random.randint(0,9))
            else: param_base += str(random.randint(0,9))
            attempts += 1

        if param_name:
            # Choose a function call - ensure necessary imports exist at top level
            default_call = random.choice([
                "datetime.datetime.now()", # Requires datetime import
                "str(random.random())",    # Requires random import
                "generate_default_id()"    # Requires a helper function defined globally
            ])
            param_str = f"{param_name}={default_call}"
            if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
            params_list.append(param_str)
            param_added = True
            # print(f"DEBUG: Added B008 param: {param_str}", file=sys.stderr)

    return params_list, param_added # Return flag indicating if a problematic param was added

# --- Function Generators ---

# MODIFIED: Make B006 harder to spot
def generate_mutable_default_arg_func(global_scope, line_num, config): # B006
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    # Use harder name generator
    func_name = gen_variable_name(global_scope, "function", f"{random.choice(VERBS)}_with_mutable", violation_prob)
    if not global_scope.define(func_name, 'function', line_num): func_name = f"fallback_mutable_func_{random.randint(100,999)}"

    params_list = []
    param_names_in_sig = set()
    mutable_param_name = None

    # Add regular parameters first
    num_regular_params = random.randint(0, 1)
    for i in range(num_regular_params):
        p_name = gen_variable_name(func_scope, "parameter", f"config_{i}", violation_prob)
        if func_scope.define(p_name, 'any', current_line_num):
            if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
            params_list.append(p_name)
            param_names_in_sig.add(p_name)

    # Try to add the mutable param
    params_list, added = _add_problematic_param_if_needed(params_list, func_scope, current_line_num, config, param_names_in_sig)
    if added:
        # Extract the name of the added mutable param (assuming it's the last one added)
        last_param_str = params_list[-1].strip()
        match = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=", last_param_str)
        if match:
            mutable_param_name = match.group(1)
            # print(f"DEBUG: Identified mutable param name: {mutable_param_name}", file=sys.stderr)

    # Ensure at least one mutable param exists if generation failed
    if not mutable_param_name:
        param_base = "forced_mutable_items"
        attempts = 0
        while attempts < 5:
            potential_name = gen_variable_name( func_scope, "parameter", param_base, 0.0 )
            if potential_name not in param_names_in_sig and func_scope.define(potential_name, 'list', line_num):
                mutable_param_name = potential_name; param_names_in_sig.add(mutable_param_name); break
            else: param_base += str(random.randint(0,9))
            attempts += 1
        if mutable_param_name:
            default_val = random.choice(['[]', '{}'])
            if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
            params_list.append(f"{mutable_param_name}={default_val}")
        else: # Last resort
             mutable_param_name = f"p_mut_{random.randint(0,9)}"
             if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
             params_list.append(f"{mutable_param_name}=[]")

    params_str = "".join(params_list)
    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    if should_violate(violation_prob * 0.3): def_keyword = "def  "
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0)
    # Potentially misleading docstring
    docstring = f"\"\"\"Processes items using configuration. May update state.\"\"\"" if random.random() < 0.4 else f"\"\"\"Handles data based on input parameters (B006?)\"\"\""
    func_lines.append(f"{doc_indent_str}{docstring}"); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope
    use_indent = gen_indent_spaces(1, 0)
    body_lines = []

    # Conditionally modify the mutable default
    if mutable_param_name and func_scope.is_defined(mutable_param_name):
        func_scope.mark_used(mutable_param_name) # Mark used now
        body_lines.append(f"{use_indent}# Potentially modifies default arg '{mutable_param_name}'")
        current_line_num += 1
        # Use another parameter in the condition if possible
        condition = "random.random() < 0.6" # Default condition
        regular_params = [p for p in param_names_in_sig if p != mutable_param_name]
        if regular_params:
            cond_param = random.choice(regular_params)
            func_scope.mark_used(cond_param) # Mark condition param used
            condition = f"{cond_param} is not None and {cond_param} > 0" # Example condition
            # Define the condition param if it wasn't defined with a default
            if not any(f"{cond_param}=" in p for p in params_list):
                 # This case shouldn't happen with current logic, but as safety:
                 pass # Assume it's passed in

        body_lines.append(f"{use_indent}if {condition}:")
        mod_indent = gen_indent_spaces(2, 0)
        # Modification depends on inferred type
        param_type = func_scope.get_var_type(mutable_param_name)
        modification = f"{mutable_param_name}.append('modified_{random.randint(100,999)}')" # Default list append
        if param_type == 'dict':
            modification = f"{mutable_param_name}['new_key_{random.randint(1,9)}'] = True"
        body_lines.append(f"{mod_indent}{modification} # B006 side-effect")
        body_lines.append(f"{use_indent}else:")
        body_lines.append(f"{mod_indent}pass # No modification this time")
        current_line_num += 4
    else:
        body_lines.append(f"{use_indent}print('Processing without default modification...')")
        current_line_num += 1

    # Add more code
    more_lines, next_ln = generate_code_block( func_scope, current_line_num, random.randint(1, 3), body_config, allow_complex=False )
    body_lines.extend(more_lines); current_line_num = next_ln
    func_lines.extend(body_lines)
    ret_indent = gen_indent_spaces(1, 0)
    # Return the modified mutable or something else
    ret_val = mutable_param_name if mutable_param_name and func_scope.is_defined(mutable_param_name) and random.random() < 0.7 else "None"
    func_lines.append(f"{ret_indent}return {ret_val}"); current_line_num += 1
    return func_lines, current_line_num

# NEW: Generator for B008
def generate_function_call_default_arg_func(global_scope, line_num, config): # B008
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    func_name = gen_variable_name(global_scope, "function", f"{random.choice(VERBS)}_with_dynamic_default", violation_prob)
    if not global_scope.define(func_name, 'function', line_num): func_name = f"fallback_dynamic_func_{random.randint(100,999)}"

    params_list = []
    param_names_in_sig = set()
    dynamic_param_name = None

    # Add regular parameters first
    num_regular_params = random.randint(0, 1)
    for i in range(num_regular_params):
        p_name = gen_variable_name(func_scope, "parameter", f"setting_{i}", violation_prob)
        if func_scope.define(p_name, 'any', current_line_num):
            if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
            params_list.append(p_name)
            param_names_in_sig.add(p_name)

    # Try to add the dynamic default param (B008)
    params_list, added = _add_problematic_param_if_needed(params_list, func_scope, current_line_num, config, param_names_in_sig)
    if added:
        last_param_str = params_list[-1].strip()
        match = re.match(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=", last_param_str)
        if match and "=" in last_param_str and "(" in last_param_str: # Check it looks like a call
            dynamic_param_name = match.group(1)
            # print(f"DEBUG: Identified B008 param name: {dynamic_param_name}", file=sys.stderr)

    # Ensure at least one dynamic param exists if generation failed
    if not dynamic_param_name:
        param_base = "forced_dynamic_val"
        attempts = 0
        while attempts < 5:
            potential_name = gen_variable_name( func_scope, "parameter", param_base, 0.0 )
            if potential_name not in param_names_in_sig and func_scope.define(potential_name, 'any', line_num):
                dynamic_param_name = potential_name; param_names_in_sig.add(dynamic_param_name); break
            else: param_base += str(random.randint(0,9))
            attempts += 1
        if dynamic_param_name:
            default_call = "datetime.datetime.now()" # Fallback default call
            if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
            params_list.append(f"{dynamic_param_name}={default_call}")
        else: # Last resort
             dynamic_param_name = f"p_dyn_{random.randint(0,9)}"
             if params_list: params_list.append(gen_whitespace_after_comma(violation_prob))
             params_list.append(f"{dynamic_param_name}=datetime.datetime.now()")


    params_str = "".join(params_list)
    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0)
    func_lines.append(f"{doc_indent_str}\"\"\"Function demonstrating B008 (function call in default).\"\"\""); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope
    use_indent = gen_indent_spaces(1, 0)
    body_lines = []

    # Use the dynamic parameter
    if dynamic_param_name and func_scope.is_defined(dynamic_param_name):
        func_scope.mark_used(dynamic_param_name)
        body_lines.append(f"{use_indent}print(f'Dynamic default value received: {{{dynamic_param_name}}}')")
        current_line_num += 1
        # Maybe do something with it
        if random.random() < 0.5:
            temp_var = gen_variable_name(func_scope, "variable", "processed_dynamic", violation_prob)
            if func_scope.define(temp_var, 'any', current_line_num):
                func_scope.mark_used(temp_var)
                body_lines.append(f"{use_indent}{temp_var} = str({dynamic_param_name}) + '_processed'")
                current_line_num += 1

    # Add more code
    more_lines, next_ln = generate_code_block( func_scope, current_line_num, random.randint(1, 2), body_config, allow_complex=False )
    body_lines.extend(more_lines); current_line_num = next_ln
    func_lines.extend(body_lines)
    ret_indent = gen_indent_spaces(1, 0)
    ret_val = dynamic_param_name if dynamic_param_name and func_scope.is_defined(dynamic_param_name) and random.random() < 0.6 else "'Default processed'"
    func_lines.append(f"{ret_indent}return {ret_val}"); current_line_num += 1
    return func_lines, current_line_num


# MODIFIED: Make SIM21x harder
def generate_explicit_bool_comparison(current_scope, line_num, config): # SIM21x/22x
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']; lines = []; current_line_num = line_num
    bool_var = None
    # Try to find an existing boolean
    existing_bools = current_scope.get_all_defined_vars(type_filter='bool')
    if existing_bools and random.random() < 0.6:
        bool_var = current_scope.get_random_defined_var(type_filter='bool') # Marks used

    # If not found or randomly decided, create one with a more complex source
    if not bool_var:
        bool_var_base = f"status_flag_{random.randint(0,9)}"
        bool_var = gen_variable_name(current_scope, "variable", bool_var_base, violation_prob * 0.5)
        assign_indent = gen_indent_spaces(config['indent_level'], 0)
        if not current_scope.define(bool_var, 'bool', current_line_num):
             return [], line_num # Give up if cannot define

        # Assign based on a comparison or simulated function
        source_expr = random.choice(['True', 'False']) # Fallback
        num_var = current_scope.get_random_defined_var(type_filter='int') or current_scope.get_random_defined_var(type_filter='float')
        if num_var and random.random() < 0.6:
            op = random.choice(['>', '<', '==', '!='])
            threshold = random.randint(0, 100)
            source_expr = f"{num_var} {op} {threshold}"
            current_scope.mark_used(num_var) # Mark the source var used
        elif random.random() < 0.3:
             # Simulate a function call returning bool (function doesn't need to exist for generation)
             check_func = gen_variable_name(current_scope, "function", "check_condition", 0) # Assume func exists
             source_expr = f"{check_func}()"

        lines.append(f"{assign_indent}{bool_var} = {source_expr}")
        current_line_num += 1
        current_scope.mark_used(bool_var) # Used in the comparison below

    # Now bool_var is defined (or was existing)
    comparison_val = random.choice(['True', 'False']); op = random.choice(['==', '!='])
    op_str = gen_whitespace_around_op(op, violation_prob * 1.1); condition = f"{bool_var}{op_str}{comparison_val}"
    if_keyword = "if"
    if should_violate(violation_prob * 0.3): if_keyword = "if  "
    lines.append(f"{indent_str}{if_keyword} {condition}: # SIM21x/SIM22x violation")
    body_start_line = current_line_num + 1
    body_config = copy.deepcopy(config); body_config['indent_level'] += 1; body_config['current_scope'] = current_scope
    # Add more complex body
    body_lines, next_ln = generate_code_block( current_scope, body_start_line, random.randint(1, 4), body_config, allow_complex=True ) # Allow nesting
    lines.extend(body_lines); current_line_num = next_ln
    # Add else/elif more often
    if random.random() < 0.6:
        else_indent_str = gen_indent_spaces(config['indent_level'], 0); else_keyword = random.choice(["else", "elif"])
        if else_keyword == "elif":
            # Generate another (potentially complex) condition for elif
            other_cond_var = current_scope.get_random_defined_var(type_filter='int')
            if other_cond_var:
                 elif_cond = f"{other_cond_var} < {random.randint(0,50)}"
                 current_scope.mark_used(other_cond_var)
            else: elif_cond = f"{random.choice(['True', 'False'])}" # Fallback elif condition
            lines.append(f"{else_indent_str}elif {elif_cond}:")
        else:
            lines.append(f"{else_indent_str}else:");

        else_body_start_line = current_line_num + 1
        else_body_config = copy.deepcopy(config); else_body_config['indent_level'] += 1; else_body_config['current_scope'] = current_scope
        else_body, next_ln_else = generate_code_block( current_scope, else_body_start_line, random.randint(1, 3), else_body_config, allow_complex=False )
        lines.extend(else_body); current_line_num = next_ln_else
    return lines, current_line_num

# MODIFIED: Make SIM108 harder
def generate_if_else_assignment(current_scope, line_num, config): # SIM108
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']; lines = []; current_line_num = line_num
    # Get/create a boolean condition, potentially complex
    bool_var = None
    existing_bools = current_scope.get_all_defined_vars(type_filter='bool')
    if existing_bools and random.random() < 0.5:
        bool_var = current_scope.get_random_defined_var(type_filter='bool')

    condition_expr = "UNKNOWN_CONDITION"
    if bool_var:
        condition_expr = bool_var # Use existing bool var name
    else:
        # Create a condition expression directly
        num_var = current_scope.get_random_defined_var(type_filter='int') or current_scope.get_random_defined_var(type_filter='float')
        if num_var:
            op = random.choice(['>', '<', '==', '!=', '>=', '<='])
            threshold = random.randint(0, 100)
            condition_expr = f"{num_var} {op} {threshold}"
            current_scope.mark_used(num_var)
        else:
            # Fallback: create a simple bool var
            bool_var_base = f"cond_flag_{random.randint(0,9)}"
            bool_var = gen_variable_name(current_scope, "variable", bool_var_base)
            assign_indent = gen_indent_spaces(config['indent_level'], 0)
            if not current_scope.define(bool_var, 'bool', current_line_num):
                return [], line_num
            lines.append(f"{assign_indent}{bool_var} = {random.choice(['True', 'False'])}")
            current_line_num += 1
            current_scope.mark_used(bool_var)
            condition_expr = bool_var

    # Define the result variable
    result_var_base = f"result_status_{random.randint(0,9)}"
    result_var = gen_variable_name(current_scope, "variable", result_var_base, violation_prob * 0.6)

    # Make assigned values more complex
    val_type = random.choice(['int', 'str', 'float'])
    val1 = gen_simple_value(val_type)
    val2 = gen_simple_value(val_type)
    while val1 == val2: val2 = gen_simple_value(val_type)

    # Add complexity: values might involve other variables
    if val_type in ['int', 'float'] and random.random() < 0.4:
        other_num_var = current_scope.get_random_defined_var(type_filter=val_type)
        if other_num_var:
            op1 = random.choice(['+', '*', '-'])
            op2 = random.choice(['+', '*', '-'])
            val1 = f"{other_num_var} {op1} {random.randint(1, 10)}"
            val2 = f"{other_num_var} {op2} {random.randint(1, 10)}"
            current_scope.mark_used(other_num_var)

    result_type = val_type

    if not current_scope.define(result_var, result_type, current_line_num): return lines, current_line_num

    op_eq = gen_whitespace_around_op("=", violation_prob * 1.1)
    if_indent = gen_indent_spaces(config['indent_level'] + 1, 0)
    lines.append(f"{indent_str}if {condition_expr}: # Start SIM108 violation")
    lines.append(f"{if_indent}{result_var}{op_eq}{val1}")
    lines.append(f"{indent_str}else:")
    lines.append(f"{if_indent}{result_var}{op_eq}{val2} # End SIM108 violation")
    current_scope.mark_used(result_var) # Assigned to
    current_line_num += 4
    return lines, current_line_num

# MODIFIED: Make C4xx harder
def generate_redundant_comprehension(current_scope, line_num, config): # C4xx
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']; lines = []; current_line_num = line_num
    # Get/create an iterable
    list_var = current_scope.get_random_defined_var(type_filter='list') or \
               current_scope.get_random_defined_var(type_filter='set') or \
               current_scope.get_random_defined_var(type_filter='tuple') or \
               current_scope.get_random_defined_var(type_filter='str') # Include str

    source_iterable = f"range({random.randint(3,8)})" # Default
    source_type = 'range'

    if list_var:
        source_iterable = list_var
        source_type = current_scope.get_var_type(list_var) or 'any'
        current_scope.mark_used(list_var)
    else:
        # Create a new iterable if none suitable found
        list_var_base = f"source_items_{random.randint(0,9)}"
        list_var = gen_variable_name(current_scope, "variable", list_var_base)
        assign_indent = gen_indent_spaces(config['indent_level'], 0)
        iter_type = random.choice(['list', 'set', 'tuple', 'str'])
        iter_val = gen_simple_value(iter_type)
        if not current_scope.define(list_var, iter_type, current_line_num):
            return [], line_num
        lines.append(f"{assign_indent}{list_var} = {iter_val}")
        current_line_num += 1
        current_scope.mark_used(list_var)
        source_iterable = list_var
        source_type = iter_type


    new_var_base = f"redundant_collection_{list_var[:10]}_{random.randint(0,9)}"
    new_var = gen_variable_name(current_scope, "variable", new_var_base, violation_prob * 0.7)

    op_eq = gen_whitespace_around_op("=", violation_prob * 1.1);
    # Prioritize harder-to-spot C4 violations
    violation_type = random.choices(
        ['C417', 'C400', 'C402', 'C416', 'C408', 'C405', 'C406'], # C417, C416 higher weight
        weights=[4, 3, 3, 3, 2, 1, 1],
        k=1
    )[0]

    line = ""; defined = False; new_var_type = 'any'
    comment = f"# {violation_type} violation"

    # Generate code based on violation type
    try:
        if violation_type == 'C417': # map(lambda...) -> comprehension
            # Make lambda slightly more complex
            lambda_body = random.choice(["x * 2", "x + 1", "str(x)", f"x // {random.randint(2,5)}", "x % 5"])
            # Ensure source is suitable for map (list, tuple, range, str)
            if source_type not in ['list', 'tuple', 'range', 'str', 'set']: source_iterable = f"range({random.randint(3,8)})" # Fallback
            map_func = f"lambda x: {lambda_body}"
            wrapper = random.choice(['list', 'set', 'tuple']) # Wrap map result
            line = f"{indent_str}{new_var}{op_eq}{wrapper}(map({map_func}, {source_iterable}))"; new_var_type = wrapper
        elif violation_type == 'C400': # list(genexpr) -> [...]
            transform = random.choice(['i', 'i+1', 'str(i)'])
            line = f"{indent_str}{new_var}{op_eq}list({transform} for i in {source_iterable})"; new_var_type = 'list'
        elif violation_type == 'C402': # set(genexpr) -> {...}
            transform = random.choice(['i', 'i*i', 'i%10'])
            line = f"{indent_str}{new_var}{op_eq}set({transform} for i in {source_iterable})"; new_var_type = 'set'
        elif violation_type == 'C416': # comprehension instead of literal
             literal_type = random.choice(['list', 'set', 'dict'])
             if literal_type == 'list': line = f"{indent_str}{new_var}{op_eq}[i for i in [1, 2, 3]]"; new_var_type = 'list'
             elif literal_type == 'set': line = f"{indent_str}{new_var}{op_eq}{{i for i in [10, 20]}}"; new_var_type = 'set'
             else: line = f"{indent_str}{new_var}{op_eq}{{k: v for k, v in {{'a': 1, 'b': 2}}.items()}}"; new_var_type = 'dict'
        elif violation_type == 'C408': # dict(...) -> {...}
            key1 = gen_variable_name(current_scope, "kwarg", "key_a"); key2 = gen_variable_name(current_scope, "kwarg", "key_b")
            if key1 == key2: key2 += "_2"
            val1 = gen_simple_value('int'); val2 = gen_simple_value('str')
            line = f"{indent_str}{new_var}{op_eq}dict({key1}={val1}, {key2}={val2})"; new_var_type = 'dict'
        elif violation_type == 'C405': # set([...]) -> {...}
            line = f"{indent_str}{new_var}{op_eq}set([{gen_simple_value('int')}, {gen_simple_value('int')}])"; new_var_type = 'set'
        elif violation_type == 'C406': # list((...)) -> [...]
            line = f"{indent_str}{new_var}{op_eq}list(({gen_simple_value('int')}, {gen_simple_value('str')}))"; new_var_type = 'list'
        else: # Fallback (less common C4 or simple copy)
            line = f"{indent_str}{new_var}{op_eq}{source_iterable}[:]"; new_var_type = 'list'; comment="# Fallback copy"

        defined = current_scope.define(new_var, new_var_type, current_line_num)
        if defined:
            current_scope.mark_used(new_var) # Assume used later
            lines.append(line + f" {comment}")
            current_line_num += 1
        else:
            # print(f"DEBUG: Failed to define {new_var} for C4 violation", file=sys.stderr)
            pass # Skip if definition fails

    except Exception as e:
        print(f"ERROR generating C4xx line: {e}", file=sys.stderr)
        # traceback.print_exc()
        lines.append(f"{indent_str}# C4XX GEN ERROR: {e}")
        current_line_num += 1

    return lines, current_line_num


# --- Base Code Structure Generators ---
# MODIFIED: Add misleading type hints sometimes
def generate_assignment(current_scope, line_num, config):
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']
    var_name = gen_variable_name(current_scope, "variable", None, violation_prob * 1.1) # Use harder name gen
    value = gen_simple_value()
    value_type = 'any'
    try:
        eval_val = eval(value) # Use eval carefully
        value_type = type(eval_val).__name__
    except: # Handle eval failures gracefully
        if value.startswith("'") or value.startswith('"'): value_type = 'str'
        elif value == 'None': value_type = 'None'
        elif value == 'True' or value == 'False': value_type = 'bool'

    # Add potentially misleading type hint
    type_hint_str = ""
    if should_violate(violation_prob * 0.3):
        hint_options = ['Any', 'object', 'str', 'int', 'float', 'List', 'Dict', 'Optional[int]'] # Requires importing Optional
        chosen_hint = random.choice(hint_options)
        # Make hint slightly plausible but potentially wrong
        if value_type != 'any' and chosen_hint.lower() != value_type and chosen_hint not in ['Any', 'object']:
             if random.random() < 0.7: # High chance of applying misleading hint
                 type_hint_str = f": {chosen_hint}"
        elif chosen_hint not in ['Any', 'object']: # Apply if no conflict detected
             type_hint_str = f": {chosen_hint}"


    if not current_scope.define(var_name, value_type, line_num): return [], line_num
    op_str = gen_whitespace_around_op("=", violation_prob)
    line = f"{indent_str}{var_name}{type_hint_str}{op_str}{value}"
    current_scope.mark_used(var_name) # Assume used later
    return [line], line_num + 1

def generate_print(current_scope, line_num, config):
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']
    # Select from a wider range of types, including potentially complex ones
    printable_vars = current_scope.get_all_defined_vars() # Get all vars first
    printable_vars = [v for v in printable_vars if current_scope.get_var_type(v) != 'function'] # Exclude functions

    args_to_print = []
    if not printable_vars: args_to_print = [f"'Log message: {random.randint(0,100)}'"]
    else:
        num_args = random.randint(1, min(5, len(printable_vars))) # Print more args sometimes
        # Use get_random_defined_var which also marks as used
        selected_vars = []
        for _ in range(num_args):
             var = current_scope.get_random_defined_var() # Get any type
             if var: selected_vars.append(var)
        selected_vars = list(set(selected_vars)) # Unique vars

        # Add more complex f-string formatting sometimes
        if selected_vars and random.random() < 0.5:
            f_string_parts = []
            label = random.choice(VERBS).capitalize()
            f_string_parts.append(f"'{label}: ")
            for i, var_name in enumerate(selected_vars):
                 # Add formatting specifiers randomly
                 fmt_spec = random.choice(["", ":>10", ":.2f", ":<15", ":^10", "!r"]) if random.random() < 0.3 else ""
                 f_string_parts.append(f"{{{var_name}{fmt_spec}}}")
                 if i < len(selected_vars) - 1: f_string_parts.append(", ")
            f_string_parts.append("'")
            args_to_print = [f"f{''.join(f_string_parts)}"]
        elif selected_vars: # Simpler print
            args_to_print.append(f"'{random.choice(VERBS).capitalize()} Data:'")
            args_to_print.append(gen_whitespace_after_comma(violation_prob))
            for i, var_name in enumerate(selected_vars):
                args_to_print.append(var_name)
                if i < len(selected_vars) - 1: args_to_print.append(gen_whitespace_after_comma(violation_prob))
        else: # Fallback if selection failed
             args_to_print.append(f"'{random.choice(VERBS).capitalize()} Status:'")


    args_str = "".join(args_to_print)
    paren_open, paren_close = "(", ")"
    # Add more whitespace violations around parens
    if should_violate(violation_prob * 0.5): paren_open = random.choice(["( ", " ("])
    if should_violate(violation_prob * 0.5): paren_close = random.choice([" )", ") "])
    line = f"{indent_str}print{paren_open}{args_str}{paren_close}"
    return [line], line_num + 1

# MODIFIED: Generate potentially misleading comments
def generate_comment(config, text=None):
     line_num = config.get('current_line_num', 0)
     indent_level = config.get('indent_level', 0)
     violation_prob = config.get('violation_probability', 0.0)
     indent_str = gen_indent_spaces(indent_level, 0)
     if text is None:
         # Generate more varied/potentially misleading comments
         if random.random() < 0.3:
             text = f"TODO: Refactor this section - {random.choice(NOUNS)}" # Misleading TODO
         elif random.random() < 0.3:
             text = f"Temporary fix for issue #{random.randint(100,999)}" # Implies fragility
         elif random.random() < 0.2:
             text = f"Ensure {random.choice(NOUNS)} is {random.choice(ADJECTIVES)}" # Might contradict code
         else:
             text = f"{random.choice(VERBS).capitalize()} {random.choice(NOUNS)}." # Standard comment

     if not isinstance(line_num, int):
         if isinstance(config, dict): config['current_line_num'] = 0
         line_num = 0
     # Increase chance of bad spacing
     if should_violate(violation_prob * 0.6):
         return generate_bad_comment_space(config)
     else:
         line = f"{indent_str}# {text}"
         # Add chance of overly long comment
         if len(line) > TARGET_COMMENT_LENGTH and random.random() < 0.2:
             pass # Already long
         elif random.random() < 0.15:
             line += " // " + " ".join([random.choice(NOUNS) for _ in range(10)]) # Make it too long

         return [line], line_num + 1

# MODIFIED: More complex conditions and bodies
def generate_if_statement(current_scope, line_num, config):
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']
    lines = []; current_line_num = line_num

    # Generate a more complex condition
    condition_parts = []
    num_parts = random.randint(1, 3) # Combine 1 to 3 conditions
    for i in range(num_parts):
        part_expr = "False # Fallback condition"
        # Try numeric comparison
        num_var = current_scope.get_random_defined_var(type_filter='int') or current_scope.get_random_defined_var(type_filter='float')
        if num_var and random.random() < 0.5:
            op = random.choice(['>', '<', '==', '!=', '>=', '<='])
            val = gen_simple_value(current_scope.get_var_type(num_var) or 'int')
            part_expr = f"{num_var}{gen_whitespace_around_op(op, violation_prob)}{val}"
            current_scope.mark_used(num_var)
        else:
            # Try boolean variable or string check
            bool_or_str_var = current_scope.get_random_defined_var(type_filter='bool') or current_scope.get_random_defined_var(type_filter='str')
            if bool_or_str_var:
                var_type = current_scope.get_var_type(bool_or_str_var)
                if var_type == 'bool':
                    part_expr = f"{bool_or_str_var}" # Implicit bool check (good) or explicit (SIM violation)
                    if should_violate(violation_prob * 0.4): # Add explicit check sometimes
                         part_expr += f"{gen_whitespace_around_op(random.choice(['==','!=']), violation_prob)}{random.choice(['True','False'])}"
                elif var_type == 'str':
                    check_val = gen_simple_value('str')
                    op = random.choice(['==', '!=', 'in'])
                    if op == 'in': part_expr = f"{check_val}{gen_whitespace_around_op(op, violation_prob)}{bool_or_str_var}"
                    else: part_expr = f"{bool_or_str_var}{gen_whitespace_around_op(op, violation_prob)}{check_val}"
                current_scope.mark_used(bool_or_str_var)
            else:
                # Fallback: create a simple bool var if no suitable var found
                temp_bool = gen_variable_name(current_scope, "variable", f"cond_bool_{i}", violation_prob)
                if current_scope.define(temp_bool, 'bool', current_line_num):
                    lines.append(f"{indent_str}{temp_bool} = {random.choice(['True', 'False'])}")
                    current_line_num += 1
                    current_scope.mark_used(temp_bool)
                    part_expr = temp_bool
                else:
                    part_expr = random.choice(['True', 'False']) # Last resort literal

        condition_parts.append(f"({part_expr})" if num_parts > 1 else part_expr) # Add parens for clarity if multiple parts

    # Combine parts with 'and' or 'or'
    logic_op = gen_whitespace_around_op(random.choice(['and', 'or']), violation_prob)
    condition = logic_op.join(condition_parts)

    if_keyword = "if"
    if should_violate(violation_prob * 0.3): if_keyword = "if  "
    line = f"{indent_str}{if_keyword} {condition}:"; lines.append(line)

    body_start_line = current_line_num + 1
    if_body_config = copy.deepcopy(config); if_body_config['indent_level'] += 1; if_body_config['current_scope'] = current_scope
    # Generate longer, potentially nested body
    if_body, next_ln_if = generate_code_block(current_scope, body_start_line, random.randint(2, 5), if_body_config, allow_complex=True)
    lines.extend(if_body); current_line_num = next_ln_if

    # Add elif/else more often and make them complex too
    if random.random() < 0.7: # Increased chance
        else_indent_str = gen_indent_spaces(config['indent_level'], 0);
        use_elif = random.random() < 0.5
        else_keyword = "elif" if use_elif else "else"

        if use_elif:
            # Generate another complex condition for elif
            elif_cond_parts = []
            elif_num_parts = random.randint(1, 2)
            for i in range(elif_num_parts):
                 elif_part_expr = "True # Fallback elif"
                 elif_var = current_scope.get_random_defined_var(type_filter='int')
                 if elif_var:
                     op = random.choice(['<', '>=', '=='])
                     val = random.randint(-10, 10)
                     elif_part_expr = f"{elif_var} {op} {val}"
                     current_scope.mark_used(elif_var)
                 elif_cond_parts.append(elif_part_expr)
            elif_logic_op = gen_whitespace_around_op(random.choice(['and', 'or']), violation_prob)
            elif_condition = elif_logic_op.join(elif_cond_parts)
            lines.append(f"{else_indent_str}elif {elif_condition}:")
        else:
            lines.append(f"{else_indent_str}else:")

        else_body_start_line = current_line_num + 1
        else_body_config = copy.deepcopy(config); else_body_config['indent_level'] += 1; else_body_config['current_scope'] = current_scope
        # Allow complex else/elif body
        else_body, next_ln_else = generate_code_block(current_scope, else_body_start_line, random.randint(1, 4), else_body_config, allow_complex=True)
        lines.extend(else_body); current_line_num = next_ln_else
    return lines, current_line_num

# MODIFIED: More complex loop variable naming and body
def generate_for_loop(current_scope, line_num, config):
    indent_str = gen_indent_spaces(config['indent_level'], 0); violation_prob = config['violation_probability']
    lines = []; current_line_num = line_num
    # Use harder name generator for loop variable, higher chance of single letter
    loop_var_base = random.choice(['i', 'idx', 'item', 'elem', 'key', 'val', 'record', 'x', 'y', 'z']) # More single letters
    loop_var = gen_variable_name( current_scope, "variable", loop_var_base, violation_prob * 1.3 ) # Higher prob for bad name
    # Define loop var - its type depends on the iterable, default to 'any'
    if not current_scope.define(loop_var, 'any', current_line_num): return [], line_num
    # Don't mark used yet, body might not use it

    # Get/create an iterable, potentially more complex
    iterable_types = ['list', 'str', 'dict', 'set', 'tuple', 'range']
    iterable_name = None
    found_iterable = False
    for iter_type in iterable_types:
        candidates = current_scope.get_all_defined_vars(type_filter=iter_type)
        if candidates:
            iterable_name = current_scope.get_random_defined_var(type_filter=iter_type) # Marks used
            if iterable_name:
                found_iterable = True
                break

    iterable_expr = f"range({random.randint(3, 12)})" # Default safe iterable, larger range
    if found_iterable and iterable_name:
        iterable_expr = iterable_name # Use existing variable
    elif random.random() < 0.5: # Sometimes create a new literal iterable
        iter_type = random.choice(['list', 'tuple', 'set', 'str'])
        iterable_expr = gen_simple_value(iter_type)
        # Make dict iteration more explicit sometimes
        if iter_type == 'dict' and random.random() < 0.4:
            iterable_expr += random.choice(['.keys()', '.values()', '.items()'])


    for_keyword = "for"
    if should_violate(violation_prob * 0.3): for_keyword = "for  "
    in_keyword = "in"
    if should_violate(violation_prob * 0.4): in_keyword = random.choice([" in ", "  in", "in  "])
    line = f"{indent_str}{for_keyword} {loop_var} {in_keyword} {iterable_expr}:"; lines.append(line)
    body_start_line = current_line_num + 1
    loop_body_config = copy.deepcopy(config); loop_body_config['indent_level'] += 1; loop_body_config['current_scope'] = current_scope
    # Generate longer, more complex loop body, allow nesting
    loop_body, next_ln_loop = generate_code_block(current_scope, body_start_line, random.randint(2, 6), loop_body_config, allow_complex=True)

    # Check if loop variable was actually used in the generated body
    loop_var_used = False
    body_text = "\n".join(loop_body)
    # Basic check for usage (doesn't handle complex cases like f-strings well)
    if re.search(r'\b' + re.escape(loop_var) + r'\b', body_text):
        loop_var_used = True
        current_scope.mark_used(loop_var) # Mark used if found

    # If loop var wasn't used, maybe add a simple use or comment
    if not loop_var_used and loop_body:
        if random.random() < 0.6:
             print_indent = gen_indent_spaces(loop_body_config['indent_level'], 0)
             loop_body.insert(0, f"{print_indent}# Loop var '{loop_var}' might be unused?") # Add comment
             # loop_body.insert(0, f"{print_indent}print(f'Processing: {{{loop_var}}}')") # Add simple use
             # current_scope.mark_used(loop_var) # Mark used now
        else:
             # Or just let the F841 violation exist
             pass


    lines.extend(loop_body); current_line_num = next_ln_loop
    return lines, current_line_num

# NEW: Generator for global variable modification
def generate_global_modification_func(global_scope, line_num, config):
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    func_name = gen_variable_name(global_scope, "function", f"update_global_{random.choice(['counter','flag','state'])}", violation_prob)
    if not global_scope.define(func_name, 'function', line_num): func_name = f"fallback_global_mod_{random.randint(100,999)}"

    params_list = []
    param_names_in_sig = set()
    # Add a parameter that might influence the global change
    if random.random() < 0.6:
        p_name = gen_variable_name(func_scope, "parameter", "increment_value", violation_prob)
        if func_scope.define(p_name, 'int', current_line_num):
            params_list.append(p_name)
            param_names_in_sig.add(p_name)

    params_str = "".join(params_list)
    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0)
    func_lines.append(f"{doc_indent_str}\"\"\"Modifies global state (potentially bad practice).\"\"\""); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope
    body_lines = []
    use_indent = gen_indent_spaces(1, 0)

    # Choose which global to modify
    global_to_modify = random.choice(["GLOBAL_COUNTER", "GLOBAL_FLAG"])

    body_lines.append(f"{use_indent}global {global_to_modify}")
    current_line_num += 1

    # Modify the global based on parameter or randomly
    increment = "1" # Default increment
    if "increment_value" in param_names_in_sig:
        increment = "increment_value"
        func_scope.mark_used("increment_value")

    if global_to_modify == "GLOBAL_COUNTER":
        op = gen_whitespace_around_op(random.choice(["+=", "-="]), violation_prob)
        body_lines.append(f"{use_indent}{global_to_modify}{op}{increment}")
    elif global_to_modify == "GLOBAL_FLAG":
        body_lines.append(f"{use_indent}{global_to_modify} = not {global_to_modify}") # Toggle flag

    current_line_num += 1
    body_lines.append(f"{use_indent}print(f'Global {global_to_modify} is now: {{{global_to_modify}}}')")
    current_line_num += 1

    func_lines.extend(body_lines)
    ret_indent = gen_indent_spaces(1, 0)
    func_lines.append(f"{ret_indent}return {global_to_modify}"); current_line_num += 1 # Return the new global value
    return func_lines, current_line_num


# --- generate_code_block (MODIFIED Weights & Choices) ---
def generate_code_block(current_scope, start_line_num, num_lines, config, allow_complex=True):
    lines = []
    current_line_num = start_line_num
    lines_generated = 0
    num_lines = max(1, min(num_lines, MAX_BLOCK_LINES))
    local_config = copy.deepcopy(config)
    local_config['current_scope'] = current_scope
    prob = local_config.get('violation_probability', 0.0)

    # Define generators map (including new ones)
    all_generators = {
        'assignment': generate_assignment, 'print': generate_print, 'comment': generate_comment,
        'if': generate_if_statement, 'for': generate_for_loop,
        'unused_local': generate_unused_local_variable,
        'explicit_bool_if': generate_explicit_bool_comparison, # SIM21x
        'if_else_assign': generate_if_else_assignment,       # SIM108
        'redundant_comp': generate_redundant_comprehension,   # C4xx
        'long_line': generate_long_line, 'bad_comment': generate_bad_comment_space,
        'unused_import': generate_unused_import, # F401 (only at top level)
        # Add function calls within blocks if functions exist
        # 'call_existing_func': call_existing_function, # Needs implementation
        # Add specific violation generators here if needed
    }
    # MODIFIED Weights: Favor structure and harder violations
    generator_weights = {
        'assignment': 5, 'print': 2, 'comment': 1, # Less simple stuff
        'if': 5, 'for': 4, # More structure
        'unused_local': 1, # Less focus on simple F841
        'explicit_bool_if': 3, # More SIM
        'if_else_assign': 3,   # More SIM
        'redundant_comp': 3,   # More C4
        'long_line': 1, 'bad_comment': 1,
        'unused_import': 0.5 # Only relevant at top level
    }
    needs_scope_line_config = {
        'assignment', 'print', 'if', 'for', 'unused_local',
        'explicit_bool_if', 'if_else_assign', 'redundant_comp'
    }
    needs_config_only = {'comment', 'bad_comment', 'unused_import'}
    needs_indent_str_config = {'long_line'}

    while lines_generated < num_lines:
        block_lines = []
        next_line_num_after_gen = current_line_num
        gen_config = copy.deepcopy(local_config)
        gen_config['current_line_num'] = current_line_num

        # Filter available choices
        can_nest = allow_complex and local_config.get('indent_level', 0) < MAX_NESTING
        remaining_lines = num_lines - lines_generated
        available_choices = []
        current_weights = []

        # --- Add chance to call existing functions within blocks ---
        # (Simplified: assumes global scope has functions, doesn't check args yet)
        # if current_scope and current_scope.parent is None: # Check if we are in a function scope (approx)
        if local_config.get('indent_level', 0) > 0: # Allow calls inside functions/blocks
            global_funcs = config.get('global_function_names', [])
            if global_funcs and random.random() < 0.15: # Chance to call a function
                 chosen_func = random.choice(global_funcs)
                 # Simplistic call - assumes no args or handles missing ones gracefully
                 call_indent = gen_indent_spaces(local_config['indent_level'], 0)
                 # Try to find a variable to pass if func likely takes one
                 arg_to_pass = ""
                 potential_arg = current_scope.get_random_defined_var()
                 if potential_arg: arg_to_pass = potential_arg

                 block_lines = [f"{call_indent}{chosen_func}({arg_to_pass}) # Call existing func"]
                 next_line_num_after_gen = current_line_num + 1
                 # Skip normal generator selection for this iteration
                 lines.extend(block_lines)
                 lines_generated += 1
                 current_line_num = next_line_num_after_gen
                 continue # Go to next iteration of the while loop


        # --- Normal Generator Selection ---
        for choice, weight in generator_weights.items():
            if choice not in all_generators: continue
            is_complex = choice in {'if', 'for'} # Removed others as they are single statements now
            # Allow complex structures if nesting permits and enough lines remain
            if is_complex and (not can_nest or remaining_lines < 4): continue # Need more lines for complex
            # Prevent imports inside blocks
            if choice == 'unused_import' and local_config.get('indent_level', 0) > 0: continue
            # Ensure scope exists if needed
            if choice in needs_scope_line_config and not current_scope: continue
            if choice == 'long_line' and not current_scope: continue

            available_choices.append(choice)
            current_weights.append(weight)

        if not available_choices or not current_weights: break # Exit if no choices left
        # Ensure weights sum is positive if list is not empty
        if not available_choices: break
        if sum(current_weights) <= 0: current_weights = [1] * len(available_choices) # Fallback weights

        choice = random.choices(available_choices, weights=current_weights, k=1)[0]

        # --- Generate Code ---
        try:
            gen_func = all_generators[choice]
            block_lines = []
            next_line_num_after_gen = current_line_num

            if choice in needs_scope_line_config:
                block_lines, next_line_num_after_gen = gen_func(current_scope, current_line_num, gen_config)
            elif choice in needs_config_only:
                block_lines, next_line_num_after_gen = gen_func(gen_config)
            elif choice in needs_indent_str_config:
                 indent_str = gen_indent_spaces(gen_config['indent_level'], 0)
                 block_lines, next_line_num_after_gen = gen_func(indent_str, gen_config)
            else:
                 raise ValueError(f"Internal Error: Generator choice '{choice}' has unhandled argument needs.")

            # Sanity check returns
            if not isinstance(block_lines, list):
                print(f"CRITICAL WARNING: Gen '{choice}' did not return list! Got {type(block_lines)}.", file=sys.stderr)
                block_lines = []
            if not isinstance(next_line_num_after_gen, int) or next_line_num_after_gen < current_line_num:
                 # print(f"CRITICAL WARNING: Gen '{choice}' returned invalid line num! Got {next_line_num_after_gen}, expected >= {current_line_num}.", file=sys.stderr)
                 next_line_num_after_gen = current_line_num + len(block_lines) if block_lines else current_line_num + 1

            # Post-process last generated line (less aggressive trailing whitespace)
            if block_lines and isinstance(block_lines, list) and block_lines[-1].strip():
                last_line = block_lines[-1]
                if should_violate(prob * 0.1): # Lower chance
                     last_line = generate_trailing_whitespace(last_line) # W291
                elif should_violate(prob * 0.1) and '#' not in last_line: # Lower chance
                     inline_comment_text = f"# Inline comment {random.randint(100,999)}"
                     last_line = f"{last_line.rstrip()}  {inline_comment_text}" # E261
                block_lines[-1] = last_line

        except Exception as e:
            print(f"ERROR in generator '{choice}' line ~{current_line_num} indent={gen_config.get('indent_level','?')}: {e}", file=sys.stderr)
            traceback.print_exc() # More debugging info
            indent_str = gen_indent_spaces(gen_config.get('indent_level',0), 0)
            block_lines = [f"{indent_str}# GENERATOR ERROR: {e}"]
            next_line_num_after_gen = current_line_num + 1

        # --- Append lines and update counters ---
        if block_lines:
            lines.extend(block_lines)
            new_lines_count = next_line_num_after_gen - current_line_num
            if new_lines_count <= 0: new_lines_count = len(block_lines) # Ensure progress
            lines_generated += new_lines_count
            current_line_num = next_line_num_after_gen
        else:
             # Increment even if generator produced no lines to avoid infinite loop
             lines_generated += 1
             current_line_num += 1 # Still advance line number conceptually
             if not choice: break # Exit if choice was somehow empty

    # Add pass if block is empty and requires it (e.g., inside if/for/def)
    indent_level = local_config.get('indent_level', 0)
    # Check if the block *needs* a pass (e.g., after 'if:', 'def:', 'for:')
    # This check is simplified; a more robust check would look at the context
    if indent_level > 0 and not _contains_executable(lines):
        indent_str = gen_indent_spaces(indent_level, 0)
        # Avoid adding pass if the block was intentionally empty
        # Heuristic: if num_lines was > 0 but generated nothing, add pass.
        if num_lines > 0:
            lines.append(f"{indent_str}pass # Added fallback pass")
            current_line_num += 1
    return lines, current_line_num

# --- Scenario Function Generators (MODIFIED to use harder elements) ---

def generate_load_data_func(global_scope, line_num, config):
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    func_name = gen_variable_name(global_scope, "function", "load_data_source", violation_prob * 1.1)
    if not global_scope.define(func_name, 'function', line_num): func_name = f"f_load_{random.randint(0,9)}"

    param_names_in_sig = set()
    param_name = gen_variable_name(func_scope, "parameter", "source_path", violation_prob)
    defined_primary = func_scope.define(param_name, 'str', current_line_num)
    if defined_primary: param_names_in_sig.add(param_name)
    else: param_name = 'p_path'

    params_list = [param_name] if defined_primary else []
    # Add problematic params
    params_list, _ = _add_problematic_param_if_needed(params_list, func_scope, current_line_num, config, param_names_in_sig)
    params_str = "".join(params_list)

    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    if should_violate(violation_prob * 0.3): def_keyword = "def  "
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0)
    # Potentially misleading docstring
    docstring = f"\"\"\"Loads the data records from the specified source ({param_name}).\"\"\"" if random.random() < 0.6 else f"\"\"\"Retrieve and parse input stream.\"\"\""
    func_lines.append(f"{doc_indent_str}{docstring}"); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope

    if defined_primary:
        # Use the parameter in a slightly more complex way
        print_lines, next_ln = generate_print(func_scope, current_line_num, body_config); func_lines.extend(print_lines); current_line_num = next_ln
        func_scope.mark_used(param_name)

    # Generate more complex data structure
    data_var_name = gen_variable_name(func_scope, "variable", "raw_data_struct", violation_prob)
    assign_indent = gen_indent_spaces(1, 0); list_config = copy.deepcopy(body_config);
    # Generate list of dicts or sometimes just a dict
    data_literal = gen_list_of_dicts(random.randint(2, 5), list_config) if random.random() < 0.8 else gen_simple_value('dict')
    data_type = 'list' if data_literal.strip().startswith('[') else 'dict'

    assign_op = gen_whitespace_around_op("=", violation_prob)
    if not func_scope.define(data_var_name, data_type, current_line_num): data_var_name = f"v_data_{random.randint(0,9)}"
    else: func_scope.mark_used(data_var_name) # Used as return value

    func_lines.append(f"{assign_indent}{data_var_name}{assign_op}{data_literal}"); current_line_num += data_literal.count('\n') + 1
    body_config['current_line_num'] = current_line_num
    # Add more code inside
    more_lines, next_ln = generate_code_block(func_scope, current_line_num, random.randint(0, 2), body_config, allow_complex=False)
    func_lines.extend(more_lines); current_line_num = next_ln

    ret_indent = gen_indent_spaces(1, 0)
    func_lines.append(f"{ret_indent}return {data_var_name}"); current_line_num += 1
    return func_lines, current_line_num

def generate_validate_data_func(global_scope, line_num, config):
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    func_name = gen_variable_name( global_scope, "function", "validate_records", violation_prob * 1.1);
    if not global_scope.define(func_name, 'function', line_num): func_name = f"f_valid_{random.randint(0,9)}"

    param_names_in_sig = set()
    param_name = gen_variable_name( func_scope, "parameter", "data_list", violation_prob );
    defined_primary = func_scope.define(param_name, 'list', current_line_num) # Assume list of dicts
    if defined_primary: param_names_in_sig.add(param_name)
    else: param_name = 'p_list'

    params_list = [param_name] if defined_primary else []
    params_list, _ = _add_problematic_param_if_needed(params_list, func_scope, current_line_num, config, param_names_in_sig);
    params_str = "".join(params_list)

    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    if should_violate(violation_prob*0.3): def_keyword = "def  "
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0); func_lines.append(f"{doc_indent_str}\"\"\"Checks data integrity and filters items.\"\"\""); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope; body_lines = []

    # Use list comprehension for filtering (potentially less readable than explicit loop for complex logic)
    valid_list_name = gen_variable_name( func_scope, "variable", "valid_items", violation_prob );
    if not func_scope.define(valid_list_name, 'list', current_line_num): valid_list_name = f"v_valid_{random.randint(0,9)}"
    else: func_scope.mark_used(valid_list_name) # Used in return

    reject_count_name = gen_variable_name( func_scope, "variable", "reject_count", violation_prob);
    if not func_scope.define(reject_count_name, 'int', current_line_num): reject_count_name = f"v_reject_{random.randint(0,9)}"
    else: func_scope.mark_used(reject_count_name) # Used in print
    count_indent = gen_indent_spaces(1, 0); assign_op_c = gen_whitespace_around_op("=", violation_prob);
    body_lines.append(f"{count_indent}{reject_count_name}{assign_op_c}0"); current_line_num += 1

    list_indent = gen_indent_spaces(1, 0)
    assign_op_1 = gen_whitespace_around_op("=", violation_prob)
    # Complex condition for validation
    threshold = random.randint(30,70)
    key_to_check = "'value'" # Default key
    if random.random() < 0.3: key_to_check = f"'{random.choice(['age', 'score', 'quantity'])}'" # Use other keys sometimes
    default_val = 0 if key_to_check != "'value'" else 0 # Adjust default based on key maybe
    # Add another condition part
    other_cond = ""
    flag_key = random.choice(['flag', 'active', 'enabled'])
    if random.random() < 0.5:
         other_cond = f" and item.get('{flag_key}', False)" # Check a flag too

    # Use comprehension (can be harder to debug than loop)
    comprehension = f"[item for item in {param_name} if isinstance(item, dict) and item.get({key_to_check}, {default_val}) > {threshold}{other_cond}]"
    body_lines.append(f"{list_indent}{valid_list_name}{assign_op_1}{comprehension}"); current_line_num += 1
    if defined_primary: func_scope.mark_used(param_name) # Used in comprehension

    # Calculate rejects (less efficient than doing in one loop)
    len_check = f"len({param_name} or [])" # Handle None case
    body_lines.append(f"{count_indent}{reject_count_name}{assign_op_c}{len_check} - len({valid_list_name})"); current_line_num += 1

    # Add some unrelated code block
    more_lines, next_ln = generate_code_block(func_scope, current_line_num, random.randint(0, 1), body_config, allow_complex=False)
    body_lines.extend(more_lines); current_line_num = next_ln

    print_indent = gen_indent_spaces(1, 0);
    paren_open, paren_close = "(", ")"
    if should_violate(violation_prob * 0.3): paren_open = "( "; paren_close = " )"
    body_lines.append(f"{print_indent}print{paren_open}f'Validation complete. Kept: {{len({valid_list_name})}}, Rejected: {{{reject_count_name}}}'{paren_close}"); current_line_num += 1
    func_lines.extend(body_lines); ret_indent = gen_indent_spaces(1, 0); func_lines.append(f"{ret_indent}return {valid_list_name}"); current_line_num += 1
    return func_lines, current_line_num


def generate_analyze_data_func(global_scope, line_num, config):
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    func_name = gen_variable_name( global_scope, "function", "calculate_stats", violation_prob * 1.1)
    if not global_scope.define(func_name, 'function', line_num): func_name = f"f_analyze_{random.randint(0,9)}"

    param_names_in_sig = set()
    param_name = gen_variable_name( func_scope, "parameter", "data_set", violation_prob )
    # Assume list, but could be dict sometimes
    param_type = 'list' if random.random() < 0.8 else 'dict'
    defined_primary = func_scope.define(param_name, param_type, current_line_num)
    if defined_primary: param_names_in_sig.add(param_name)
    else: param_name = 'p_data'

    params_list = [param_name] if defined_primary else []
    params_list, _ = _add_problematic_param_if_needed(params_list, func_scope, current_line_num, config, param_names_in_sig);
    params_str = "".join(params_list)

    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    if should_violate(violation_prob*0.3): def_keyword = "def  "
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0); func_lines.append(f"{doc_indent_str}\"\"\"Computes aggregate metrics from the dataset.\"\"\""); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope; body_lines = []

    # Initial check - make it slightly more obscure
    check_indent = gen_indent_spaces(1, 0);
    if defined_primary: func_scope.mark_used(param_name);
    # Check for emptiness in a less direct way
    check_line = f"{check_indent}if not {param_name} or len({param_name}) == 0:"
    body_lines.append(check_line); current_line_num += 1
    ret_none_indent = gen_indent_spaces(2, 0); body_lines.append(f"{ret_none_indent}print('Warning: Empty or invalid data set provided.')"); body_lines.append(f"{ret_none_indent}return {{'total': 0, 'count': 0, 'average': 0.0}}"); current_line_num += 2 # Return more fields

    # Initialize multiple accumulators
    total_val_name = gen_variable_name( func_scope, "variable", "total_value_acc", violation_prob );
    count_name = gen_variable_name( func_scope, "variable", "item_count_acc", violation_prob );
    sum_sq_name = gen_variable_name( func_scope, "variable", "sum_squares_acc", violation_prob ); # For std dev later?

    if not func_scope.define(total_val_name, 'float', current_line_num): total_val_name = f"v_total_{random.randint(0,9)}"
    else: func_scope.mark_used(total_val_name)
    if not func_scope.define(count_name, 'int', current_line_num): count_name = f"v_count_{random.randint(0,9)}"
    else: func_scope.mark_used(count_name)
    if not func_scope.define(sum_sq_name, 'float', current_line_num): sum_sq_name = f"v_sumsq_{random.randint(0,9)}"
    else: func_scope.mark_used(sum_sq_name)

    acc_indent= gen_indent_spaces(1, 0); assign_op = gen_whitespace_around_op("=", violation_prob);
    body_lines.append(f"{acc_indent}{total_val_name}{assign_op}0.0"); current_line_num += 1
    body_lines.append(f"{acc_indent}{count_name}{assign_op}0"); current_line_num += 1
    body_lines.append(f"{acc_indent}{sum_sq_name}{assign_op}0.0"); current_line_num += 1


    loop_var = gen_variable_name(func_scope, "variable", "row_item", violation_prob * 1.2) # Harder name
    if not func_scope.define(loop_var, 'dict', current_line_num): loop_var = 'i' # Assume dict row
    else: func_scope.mark_used(loop_var) # Used in loop body
    loop_indent = gen_indent_spaces(1, 0)
    for_keyword = "for"
    if should_violate(violation_prob * 0.3): for_keyword = "for  "
    in_keyword = "in"
    if should_violate(violation_prob * 0.3): in_keyword = "  in  "
    body_lines.append(f"{loop_indent}{for_keyword} {loop_var} {in_keyword} {param_name}:"); current_line_num += 1
    lbody_indent = gen_indent_spaces(2, 0); op_inc = gen_whitespace_around_op("+=", violation_prob);

    # More complex processing inside loop
    # Safer access with default, potentially converting type
    key_to_use = random.choice(['value', 'amount', 'metric'])
    val_access = f"{loop_var}.get('{key_to_use}', 0)"
    # Add a try-except block for potential type errors (makes debugging harder)
    body_lines.append(f"{lbody_indent}try:")
    try_indent = gen_indent_spaces(3, 0)
    # Force conversion, might fail
    current_val_var = gen_variable_name(func_scope, "variable", "current_numeric_val", violation_prob)
    if func_scope.define(current_val_var, 'float', current_line_num):
        func_scope.mark_used(current_val_var)
        body_lines.append(f"{try_indent}{current_val_var} = float({val_access})")
        body_lines.append(f"{try_indent}{total_val_name}{op_inc}{current_val_var}");
        body_lines.append(f"{try_indent}{count_name}{op_inc}1");
        body_lines.append(f"{try_indent}{sum_sq_name}{op_inc}{current_val_var} ** 2");
        current_line_num += 4
    else: # Fallback if define fails
        body_lines.append(f"{try_indent}pass # Define failed")
        current_line_num += 1

    body_lines.append(f"{lbody_indent}except (ValueError, TypeError) as e:")
    except_indent = gen_indent_spaces(3, 0)
    body_lines.append(f"{except_indent}print(f'Skipping invalid data: {{{loop_var}}}, error: {{e}}') # Handle errors")
    current_line_num += 2


    # Prepare results dictionary - potentially with calculation inside
    results_name = gen_variable_name( func_scope, "variable", "analysis_results", violation_prob )
    if not func_scope.define(results_name, 'dict', current_line_num): results_name = f"v_stats_{random.randint(0,9)}"
    else: func_scope.mark_used(results_name) # Used in return
    res_indent = gen_indent_spaces(1, 0); assign_op_res = gen_whitespace_around_op("=", violation_prob);

    # Calculate average inside the dictionary creation (can hide division by zero)
    avg_expr = f"({total_val_name} / {count_name}) if {count_name} > 0 else 0.0"

    dict_indent = gen_indent_spaces(1, 0); item_indent = gen_indent_spaces(2, 0); body_lines.append(f"{dict_indent}{results_name}{assign_op_res}{{")
    body_lines.append(f"{item_indent}'total': {total_val_name},");
    body_lines.append(f"{item_indent}'count': {count_name},");
    body_lines.append(f"{item_indent}'average': {avg_expr}, # Calculation in dict");
    # Add std dev calculation (more complex, prone to errors if count is low)
    std_dev_expr = f"(({sum_sq_name} / {count_name} - ({avg_expr})**2)**0.5) if {count_name} > 1 else 0.0"
    body_lines.append(f"{item_indent}'std_dev': {std_dev_expr} # Complex calc");
    body_lines.append(f"{dict_indent}}}")
    current_line_num += 6 # Increased lines for dict

    func_lines.extend(body_lines); ret_indent = gen_indent_spaces(1, 0); func_lines.append(f"{ret_indent}return {results_name}"); current_line_num += 1
    return func_lines, current_line_num


def generate_report_func(global_scope, line_num, config):
    # This function remains relatively simple, focusing on using results
    # Hardness comes from the data it receives from analyze_data
    violation_prob = config['violation_probability']
    func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    func_name = gen_variable_name( global_scope, "function", "print_summary_report", violation_prob );
    if not global_scope.define(func_name, 'function', line_num): func_name = f"f_report_{random.randint(0,9)}"

    param_names_in_sig = set()
    param_name = gen_variable_name( func_scope, "parameter", "stats_dict_input", violation_prob );
    defined_primary = func_scope.define(param_name, 'dict', current_line_num) # Assume dict
    if defined_primary: param_names_in_sig.add(param_name)
    else: param_name = 'p_stats'

    params_list = [param_name] if defined_primary else []
    params_list, _ = _add_problematic_param_if_needed(params_list, func_scope, current_line_num, config, param_names_in_sig);
    params_str = "".join(params_list)

    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    if should_violate(violation_prob*0.3): def_keyword = "def  "
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0); func_lines.append(f"{doc_indent_str}\"\"\"Formats and prints the analysis results.\"\"\""); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope; body_lines = []

    if defined_primary: func_scope.mark_used(param_name); # Used to access stats

    # Safer access with defaults, using potentially awkward variable names
    total_val_access = f"{param_name}.get(\"total\", 0)";
    count_access = f"{param_name}.get(\"count\", 0)"
    avg_access = f"{param_name}.get(\"average\", 0.0)"
    std_dev_access = f"{param_name}.get(\"std_dev\", 0.0)" # Access new field

    # Add intermediate processing step with confusing names
    report_title_var = gen_variable_name(func_scope, "variable", "report_header_str", violation_prob)
    if func_scope.define(report_title_var, 'str', current_line_num):
        func_scope.mark_used(report_title_var)
        body_lines.append(f"{gen_indent_spaces(1,0)}{report_title_var} = '--- Analysis Report ---'")
        current_line_num += 1
    else: report_title_var = "'--- Report ---'" # Fallback

    processed_count_var = gen_variable_name(func_scope, "variable", "num_records_processed", violation_prob)
    if func_scope.define(processed_count_var, 'int', current_line_num):
        func_scope.mark_used(processed_count_var)
        body_lines.append(f"{gen_indent_spaces(1,0)}{processed_count_var} = int({count_access})") # Explicit cast
        current_line_num += 1
    else: processed_count_var = count_access # Fallback

    # Print using f-strings and accessed values
    prn_indent = gen_indent_spaces(1, 0); paren_o, paren_c = "(", ")"
    if should_violate(violation_prob*0.2): paren_o = "( "; paren_c = " )"
    l1 = f"{prn_indent}print{paren_o}{report_title_var}{paren_c}";
    l2 = f"{prn_indent}print{paren_o}f'Processed Items: {{{processed_count_var}}}'{paren_c}"
    l3 = f"{prn_indent}print{paren_o}f'Aggregated Value: {{{total_val_access}:.3f}}'{paren_c}"; # Different precision
    l4 = f"{prn_indent}print{paren_o}f'Mean Value: {{{avg_access}:.3f}}'{paren_c}";
    l5 = f"{prn_indent}print{paren_o}f'Std Deviation: {{{std_dev_access}:.3f}}'{paren_c}"; # Print std dev
    l6 = f"{prn_indent}print{paren_o}'--- End of Report ---'{paren_c}";
    body_lines.extend([l1, l2, l3, l4, l5, l6]); current_line_num += 6

    # Add unrelated code block
    more_lines, next_ln = generate_code_block(func_scope, current_line_num, random.randint(0, 1), body_config, allow_complex=False)
    body_lines.extend(more_lines); current_line_num = next_ln

    func_lines.extend(body_lines); ret_indent = gen_indent_spaces(1, 0);
    # Return something less obvious
    ret_val = f"{processed_count_var} > 0" if random.random() < 0.5 else "None"
    func_lines.append(f"{ret_indent}return {ret_val}"); current_line_num += 1
    return func_lines, current_line_num


def generate_generic_func(global_scope, line_num, config):
    # Keep this relatively simple, but use harder naming and structure inside
    violation_prob = config['violation_probability']; func_scope = Scope(parent=global_scope); func_lines = []; current_line_num = line_num
    base_name = f"{random.choice(VERBS)}_{random.choice(NOUNS)}_utility"
    func_name = gen_variable_name( global_scope, "function", base_name, violation_prob * 1.1)
    if not global_scope.define(func_name, 'function', line_num): func_name = f"f_gen_{random.randint(0,9)}"

    num_params = random.randint(0, 3); params = []; param_names_only = []
    param_names_in_sig = set()

    for i in range(num_params):
        param_base = f"input_{random.choice(NOUNS)}_{i}"
        # Generate the name and assign it to param_name
        param_name = gen_variable_name( func_scope, "parameter", param_base, violation_prob * 1.1 ) # Harder name

        # Use the generated param_name for the check
        if param_name not in param_names_in_sig:
             # Define with 'any' type or sometimes a misleading hint
             hint = 'any'
             if should_violate(violation_prob * 0.2): hint = random.choice(['str', 'int', 'object'])
             # Use the generated param_name for the definition
             if func_scope.define(param_name, hint, current_line_num):
                 # param_name is now defined, add it to the lists
                 param_names_in_sig.add(param_name)
                 param_names_only.append(param_name)
                 if params: params.append(gen_whitespace_after_comma(violation_prob))
                 # Append the parameter name itself to the list for the function signature string
                 params.append(param_name)
             # else: name collision or invalid, skip param (define failed)

    # Add problematic params
    params, _ = _add_problematic_param_if_needed(params, func_scope, current_line_num, config, param_names_in_sig);
    params_str = "".join(params)

    def_indent_str = gen_indent_spaces(0, 0); def_keyword = "def"
    if should_violate(violation_prob*0.3): def_keyword = "def  "
    func_lines.append(f"{def_indent_str}{def_keyword} {func_name}({params_str}):"); current_line_num += 1
    doc_indent_str = gen_indent_spaces(1, 0); func_lines.append(f"{doc_indent_str}\"\"\"Generic helper function with mixed logic.\"\"\""); current_line_num += 1
    body_config = copy.deepcopy(config); body_config['indent_level'] = 1; body_config['current_scope'] = func_scope
    # Generate more complex body
    body_lines, next_ln = generate_code_block( func_scope, current_line_num, random.randint(3, 8), body_config, allow_complex=True ) # More lines, complex allowed
    func_lines.extend(body_lines); current_line_num = next_ln

    # Mark defined params as used if they weren't already (simplification)
    for p_name in param_names_only:
        if not func_scope.variables[p_name]['used']:
             # Check if used by body_lines (basic check)
             body_text = "\n".join(body_lines)
             if re.search(r'\b' + re.escape(p_name) + r'\b', body_text):
                 func_scope.mark_used(p_name)

    # Mark problematic params used (simplification)
    for p_str in params:
        match = re.match(r"\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=.*)?$", p_str)
        if match:
            p_name = match.group(1)
            if p_name not in param_names_only and func_scope.is_defined(p_name) and not func_scope.variables[p_name]['used']:
                 body_text = "\n".join(body_lines)
                 if re.search(r'\b' + re.escape(p_name) + r'\b', body_text):
                      func_scope.mark_used(p_name)


    ret_indent = gen_indent_spaces(1, 0);
    # Return value based on some internal state or parameter
    ret_val = random.choice(['True', 'False', 'None', '0'])
    internal_vars = func_scope.get_all_defined_vars()
    internal_vars = [v for v in internal_vars if v not in param_names_in_sig and func_scope.get_var_type(v) != 'function']
    if internal_vars and random.random() < 0.5:
        ret_val = random.choice(internal_vars)
        func_scope.mark_used(ret_val) # Mark return var used
    elif param_names_only and random.random() < 0.3:
        ret_val = random.choice(param_names_only)
        # Already marked used if used in body

    func_lines.append(f"{ret_indent}return {ret_val}"); current_line_num += 1
    return func_lines, current_line_num

# --- Main Orchestration (MODIFIED) ---
def apply_blank_line_cleanup(lines):
    # Keep more internal blank lines, just trim start/end
    start_index = 0
    while start_index < len(lines) and not lines[start_index].strip(): start_index += 1
    end_index = len(lines)
    while end_index > start_index and not lines[end_index - 1].strip(): end_index -= 1
    # Keep one trailing blank line if original had it, for W391 check
    trimmed_lines = lines[start_index:end_index]
    # if end_index < len(lines): trimmed_lines.append("") # Keep one trailing blank
    return trimmed_lines

# Helper for B008 default value
def generate_default_id():
    return f"id_{random.randint(1000,9999)}"

def _extract_def_name(block_lines):
    """找出函数定义行并返回函数名。"""
    import re
    for ln in block_lines:
        m = re.match(r'\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(', ln)
        if m:
            return m.group(1)
    return None

def generate_code(target_lines, violation_percentage, filename):
    # ---- 先准备全局作用域和配置 ----
    global_scope = Scope()
    config = {
        "violation_probability": violation_percentage / 100.0,
        "indent_level": 0,
        "max_nesting": MAX_NESTING,
        "current_scope": global_scope,
        "global_function_names": []
    }
    all_lines = []
    current_line_num = 1
    role_to_name = {}
    func_results = {}
    imported_names = set()
    required_generators = [
        ('load',     generate_load_data_func),
        ('validate', generate_validate_data_func),
        ('analyze',  generate_analyze_data_func),
        ('report',   generate_report_func),
        ('global',   generate_global_modification_func),
    ]

    for role, gen in required_generators:
        func_code, next_ln = gen(global_scope, current_line_num, config)
        all_lines.extend(func_code); current_line_num = next_ln

        fname = _extract_def_name(func_code) or f"{role}_fallback"
        # 如果函数生成器没自己注册成功，就在这里兜底
        if not global_scope.is_defined(fname):
            global_scope.define(fname, "function", current_line_num)
        role_to_name[role] = fname
        config["global_function_names"].append(fname)

    # --- Header & Imports ---
    all_lines.extend([f"#!/usr/bin/env python3", f"# -*- coding: utf-8 -*-", ""])
    all_lines.extend([f"# Generated Python code ({filename}) - INTENTIONALLY HARD TO FIX",
                      f"# Target violation probability: {violation_percentage:.1f}%", ""])
    current_line_num += 5

    # Ensure necessary imports for B008 etc.
    base_imports = ["random", "math", "sys", "re", "io", "copy", "string", "argparse", "datetime"]
    num_imports = random.randint(5, len(base_imports)); # Import more base modules
    chosen_imports = random.sample(base_imports, num_imports)
    extra_unused_count = random.randint(3, 6); # More unused imports
    unused_imports_options = ['collections', 'os', 'functools', 'itertools', 'pathlib', 'json', 'csv', 'urllib.request', 'logging', 'heapq', 'socket', 'subprocess', 'tempfile', 'uuid', 'decimal', 'statistics']
    chosen_imports.extend(random.sample(unused_imports_options, min(extra_unused_count, len(unused_imports_options)))); random.shuffle(chosen_imports)

    # Add import violations (E401, E402)
    last_import_line = -1
    for i, mod in enumerate(chosen_imports):
         imported_names.add(mod); line = f"import {mod}"
         # E401: Multiple imports on one line
         if random.random() < config['violation_probability'] * 0.3 and last_import_line != -1 and all_lines[last_import_line].startswith("import "):
              prev_mod = all_lines[last_import_line].split('#')[0].strip().split()[-1]
              comma = gen_whitespace_after_comma(config['violation_probability'])
              all_lines[last_import_line] = f"import {prev_mod}{comma}{mod} # E401"
              # Don't update last_import_line, append to existing
         else:
              # E402: Module level import not at top of file (add blank line before sometimes)
              if random.random() < config['violation_probability'] * 0.1 and i > 0:
                   all_lines.append("")
                   current_line_num += 1
              all_lines.append(line)
              last_import_line = len(all_lines) - 1
              current_line_num += 1

    all_lines.append(""); current_line_num += 1

    # --- Add Global Variables and Helper for B008 ---
    all_lines.append(f"# --- Global State ---")
    g_counter_name = gen_variable_name(global_scope, "constant", "GLOBAL_EXEC_COUNT", config['violation_probability'])
    global_scope.define(g_counter_name, 'int', current_line_num)
    all_lines.append(f"{g_counter_name} = {random.randint(0, 10)}")
    current_line_num += 1
    g_flag_name = gen_variable_name(global_scope, "constant", "MASTER_CONTROL_FLAG", config['violation_probability'])
    global_scope.define(g_flag_name, 'bool', current_line_num)
    all_lines.append(f"{g_flag_name} = {random.choice(['True', 'False'])}")
    current_line_num += 1
    # Replace hardcoded globals with these names later if needed
    # For now, keep GLOBAL_COUNTER/FLAG simple for the global mod func

    all_lines.append("")
    all_lines.append("# Helper for B008")
    all_lines.append("def generate_default_id():")
    all_lines.append(f"    return f'uid_{random.randint(1000, 9999)}_{random.choice(string.ascii_lowercase)}'")
    global_scope.define("generate_default_id", 'function', current_line_num)
    config['global_function_names'].append("generate_default_id") # Add helper to known funcs
    all_lines.append("")
    current_line_num += 5


    # --- Top-Level Code/Violations ---
    global_block_config = copy.deepcopy(config)
    global_block_config['indent_level'] = 0
    num_global_lines = random.randint(4, 10) # More global code
    global_lines, next_ln = generate_code_block(global_scope, current_line_num, num_global_lines, global_block_config, allow_complex=True) # Allow complex global code
    all_lines.extend(global_lines); current_line_num = next_ln

    # Add specific top-level violations (less needed now block gen is complex)
    # if should_violate(config['violation_probability'] * 0.2):
    #     lines, next_ln = generate_unused_local_variable(global_scope, current_line_num, config) # F841 global
    #     all_lines.extend(lines); current_line_num = next_ln
    # if should_violate(config['violation_probability'] * 0.2):
    #      lines, next_ln = generate_unused_import(config); all_lines.extend(lines); current_line_num = next_ln # F401
    #      if lines: imported_names.add(lines[0].strip().split()[-1])

    # --- Function Generation ---

    required_generators = [
        generate_load_data_func,
        generate_validate_data_func,
        generate_analyze_data_func,
        generate_report_func,
        generate_global_modification_func,
    ]
    
    generated_func_names = set()

    for gen in required_generators:
        func_code, next_ln = gen(global_scope, current_line_num, config)
        all_lines.extend(func_code)
        current_line_num = next_ln

        fname = _extract_def_name(func_code)
        if fname:
            config['global_function_names'].append(fname)
            generated_func_names.add(fname)

    # More diverse function types
    scenario_functions = [
        generate_load_data_func,
        generate_validate_data_func,
        generate_analyze_data_func,
        generate_report_func,
        generate_mutable_default_arg_func, # B006
        generate_function_call_default_arg_func, # B008
        generate_global_modification_func, # Global mod
        generate_generic_func, # Generic filler
    ]
    random.shuffle(scenario_functions) # Generate in random order

    # Generate a base set of functions
    num_funcs_to_gen = max(4, int(target_lines / (MAX_FUNC_LINES * 1.5))) # Aim for more functions
    num_funcs_to_gen = min(num_funcs_to_gen, len(scenario_functions) * 2) # Limit repeats



    for i in range(num_funcs_to_gen):
        func_generator = random.choice(scenario_functions) # Allow repeats

        # Add blank lines (E302/E305) - more careful placement
        blanks_needed = 2; last_code_idx = -1
        for k in range(len(all_lines) - 1, -1, -1):
            line_strip = all_lines[k].strip()
            if line_strip and not line_strip.startswith('#'):
                 last_code_idx = k; break
        if last_code_idx != -1:
             blanks_needed = max(0, 2 - (len(all_lines) - 1 - last_code_idx))
             # Check if previous was class def (needs 2 lines) - simplified
             if all_lines[last_code_idx].strip().startswith("class "): blanks_needed = max(blanks_needed, 2)

        all_lines.extend([""] * blanks_needed); current_line_num += blanks_needed

        try:
            func_code, next_ln = func_generator(global_scope, current_line_num, config)
            all_lines.extend(func_code); current_line_num = next_ln
            # Extract function name and store it
            match = re.match(r"\s*def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", func_code[0])
            if match:
                fname = match.group(1)
                func_results[func_generator.__name__ + f"_{i}"] = fname # Store unique key
                if fname not in generated_func_names:
                    config['global_function_names'].append(fname) # Add to list for potential calls
                    generated_func_names.add(fname)
            else:
                 print(f"Warning: Could not parse function name from: {func_code[0]}", file=sys.stderr)

        except Exception as e:
            print(f"ERROR generating function with {func_generator.__name__}: {e}", file=sys.stderr)
            traceback.print_exc()
            all_lines.append(f"# GEN ERROR in {func_generator.__name__}: {e}"); current_line_num += 1
        if current_line_num > target_lines * 1.2: break # Stop if already way over target

    # --- Fill to Target Lines (if needed) ---
    while current_line_num < target_lines:
        # Add blank lines before next block/func
        blanks_needed = 1; last_code_idx = -1
        for i in range(len(all_lines) - 1, -1, -1):
             if all_lines[i].strip(): last_code_idx = i; break
        if last_code_idx != -1:
            is_last_func_def = all_lines[last_code_idx].strip().startswith("def ")
            blanks_needed = max(0, (2 if is_last_func_def else 1) - (len(all_lines) - 1 - last_code_idx))
        all_lines.extend([""] * blanks_needed); current_line_num += blanks_needed

        # Choose between generic func or top-level block
        choices = ['generic_func', 'module_block']; weights = [6, 4];
        struct_type = random.choices(choices, weights=weights, k=1)[0]
        block_code = []; next_ln = current_line_num
        try:
            if struct_type == 'generic_func':
                block_code, next_ln = generate_generic_func(global_scope, current_line_num, config)
                match = re.match(r"\s*def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", block_code[0])
                if match:
                    fname = match.group(1)
                    if fname not in generated_func_names:
                        config['global_function_names'].append(fname)
                        generated_func_names.add(fname)
            elif struct_type == 'module_block':
                 block_config = copy.deepcopy(config); block_config['indent_level'] = 0
                 num_block_lines = random.randint(5, MAX_BLOCK_LINES)
                 block_code, next_ln = generate_code_block( global_scope, current_line_num, num_block_lines, block_config, allow_complex=True ) # Allow complex

            all_lines.extend(block_code)
            if next_ln <= current_line_num:
                next_ln = current_line_num + len(block_code) if block_code else current_line_num + 1
            current_line_num = next_ln
        except Exception as e:
            print(f"ERROR generating fill {struct_type}: {e}", file=sys.stderr)
            traceback.print_exc()
            all_lines.append(f"# GEN ERROR in {struct_type}: {e}"); current_line_num += 1
        if current_line_num > target_lines * 1.5: print("Warning: Exceeded target lines significantly.", file=sys.stderr); break


    # --- Main Execution Block ---
    blanks_needed = 2; last_code_idx = -1
    for i in range(len(all_lines) - 1, -1, -1):
        if all_lines[i].strip(): last_code_idx = i; break
    if last_code_idx != -1: blanks_needed = max(0, 2 - (len(all_lines) - 1 - last_code_idx))
    all_lines.extend([""] * blanks_needed); current_line_num += blanks_needed

    main_lines = []; main_indent = gen_indent_spaces(0, 0)
    main_func_name = gen_variable_name(global_scope, "function", "main_entry_point", config['violation_probability'])
    if not global_scope.define(main_func_name, 'function', current_line_num): main_func_name = 'main' # Fallback
    def safe_call(func_name, arg_string=""):
        return f"{func_name}({arg_string})" if global_scope.is_defined(func_name) else "pass"

    main_lines.append(f"{main_indent}def {main_func_name}():")
    current_line_num += 1; main_scope = Scope(parent=global_scope); ln = current_line_num
    indent1 = gen_indent_spaces(1, 0)
    main_config = copy.deepcopy(config); main_config['indent_level'] = 1; main_config['current_scope'] = main_scope

    load_func   = role_to_name['load']
    valid_func  = role_to_name['validate']
    analyze_func= role_to_name['analyze']
    report_func = role_to_name['report']
    global_mod_func = role_to_name['global']

    # Mark functions used by main as used *now*
    funcs_to_call_in_main = [load_func, valid_func, analyze_func, report_func, global_mod_func]
    for func_n in funcs_to_call_in_main:
        if global_scope.is_defined(func_n):
            global_scope.mark_used(func_n)
        else:
            main_lines.append(f"{indent1}# Warning: Function '{func_n}' needed by main may not be defined.")
            ln+=1

    # Define and use variables within main, with more complexity
    data_var = gen_variable_name(main_scope, "variable", "input_dataset", config['violation_probability'])
    data_var_defined = main_scope.define(data_var, 'any', ln) # Type might be list or dict now
    if data_var_defined:
        op_eq1 = gen_whitespace_around_op("=", config['violation_probability']); src = "'./data/source.json'" # Change source
        main_lines.append(f"{indent1}{data_var}{op_eq1}"
                    f"{safe_call(load_func, src)}"); 
        ln+=1
        main_scope.mark_used(data_var)
    else: data_var = "None"; main_lines.append(f"{indent1}# Failed to define data_var"); ln+=1

    valid_var = gen_variable_name(main_scope, "variable", "filtered_data", config['violation_probability'])
    valid_var_defined = main_scope.define(valid_var, 'list', ln) # Validate func returns list
    if valid_var_defined:
        op_eq2 = gen_whitespace_around_op("=", config['violation_probability'])
        main_lines.append(f"{indent1}{valid_var}{op_eq2}"
                        f"{safe_call(valid_func, data_var)}")
        ln+=1
        main_scope.mark_used(valid_var)
    else: valid_var = "[]"; main_lines.append(f"{indent1}# Failed to define valid_var"); ln+=1

    results_var = gen_variable_name(main_scope, "variable", "computed_metrics", config['violation_probability'])
    results_var_defined = main_scope.define(results_var, 'dict', ln) # Analyze returns dict
    if results_var_defined:
        op_eq3 = gen_whitespace_around_op("=", config['violation_probability'])
        main_lines.append(f"{indent1}{results_var}{op_eq3}"
                        f"{safe_call(analyze_func, valid_var)}")
        ln+=1
        main_scope.mark_used(results_var)
    else: results_var = "{{}}"; main_lines.append(f"{indent1}# Failed to define results_var"); ln+=1

    # Call report function
    main_lines.append(f"{indent1}{safe_call(report_func, results_var)}")

    # main_lines.append(f"{indent1}{report_func}({results_var})"); 
    ln+=1

    # Call global modification function
    if global_scope.is_defined(global_mod_func):
        # Pass a value sometimes
        arg = str(random.randint(1,10)) if random.random() < 0.5 else ""
        main_lines.append(f"{indent1}{safe_call(global_mod_func, arg)}  # Modify global state")
        # main_lines.append(f"{indent1}{global_mod_func}({arg}) # Modify global state"); 
        ln+=1

    # Add potential unused var in main
    if should_violate(config['violation_probability'] * 0.3): # Lower chance
        lines_unused, ln = generate_unused_local_variable(main_scope, ln, main_config); main_lines.extend(lines_unused)

    # Add more random code in main, allow complex
    main_extra_lines, ln = generate_code_block(main_scope, ln, random.randint(2, 5), main_config, allow_complex=True)
    main_lines.extend(main_extra_lines)

    all_lines.extend(main_lines); current_line_num = ln

    # --- Add if __name__ == "__main__": ---
    blanks_needed = 2; last_code_idx = -1
    for i in range(len(all_lines) - 1, -1, -1):
        if all_lines[i].strip(): last_code_idx = i; break
    if last_code_idx != -1: blanks_needed = max(0, 2 - (len(all_lines) - 1 - last_code_idx))
    all_lines.extend([""] * blanks_needed); current_line_num += blanks_needed

    if_main_indent = gen_indent_spaces(0, 0)
    op_eq_main = gen_whitespace_around_op("==", config['violation_probability'])
    # Add violation: Yoda condition (SIM300)
    if should_violate(config['violation_probability'] * 0.3):
        if_main_line = (f"{if_main_indent}if \"__main__\"{op_eq_main}__name__:") # Yoda
    else:
        if_main_line = (f"{if_main_indent}if __name__{op_eq_main}\"__main__\":")

    all_lines.append(if_main_line); current_line_num += 1
    main_call_indent = gen_indent_spaces(1, 0)
    global_scope.mark_used(main_func_name) # Mark main func as used
    all_lines.append(f"{main_call_indent}{main_func_name}()"); current_line_num += 1

    # --- Post-process and Write ---
    processed_lines = apply_blank_line_cleanup(all_lines)

    # --- Final Usage Check (Informational - less reliable with complex code) ---
    code_as_string = "\n".join(processed_lines)
    unused_globals = []
    unused_funcs = []
    # Check global scope variables defined by the script
    for name, info in global_scope.variables.items():
        if not info['used']:
            # Basic regex check (can be fooled by comments, strings, substrings)
            # Count occurrences outside typical definition patterns
            def_patterns = [
                r"\bdef\s+" + re.escape(name) + r"\s*\(", # Function definition
                r"^\s*" + re.escape(name) + r"\s*=",      # Simple assignment at start of line
                r"\bclass\s+" + re.escape(name) + r"\b"    # Class definition
            ]
            is_definition_line = any(re.search(p, code_as_string, re.MULTILINE) for p in def_patterns)
            
            # Count non-definition occurrences
            occurrences = len(re.findall(r'\b' + re.escape(name) + r'\b', code_as_string))
            
            # Heuristic: If not marked used and appears <= 1 times (or only in definition patterns)
            # This is very approximate!
            if occurrences <= 1 or (occurrences > 1 and is_definition_line):
                 if info.get('type') == 'function':
                     if name != main_func_name and name != "generate_default_id": # Exclude main and helper
                         unused_funcs.append(name)
                 elif name not in ["GLOBAL_COUNTER", "GLOBAL_FLAG"]: # Exclude intentionally global vars
                     unused_globals.append(name)

    unused_imports_found = [];
    for imp_name in imported_names:
         # Check if used as module.attribute or just module name (not as part of another word)
         # Improved regex to avoid matching substrings
         pattern = r'(?<![a-zA-Z0-9_])' + re.escape(imp_name) + r'(?![a-zA-Z0-9_])'
         # Exclude import statements themselves
         import_pattern = r'^\s*(?:import|from)\s+' + re.escape(imp_name)
         code_without_imports = "\n".join(l for l in processed_lines if not re.match(import_pattern, l))
         if not re.search(pattern, code_without_imports):
              unused_imports_found.append(imp_name)

    # W292: No newline at end of file
    needs_final_newline = not should_violate(config['violation_probability'] * 0.4) # Increased chance

    try:
        with io.open(filename, 'w', encoding='utf-8', newline='') as f: # Use newline='' for consistency
            final_line_count = len(processed_lines)
            for i, line in enumerate(processed_lines):
                 f.write(line)
                 is_last_line = (i == final_line_count - 1)
                 # Write newline unless it's the last line AND we want the W292 violation
                 if not is_last_line or needs_final_newline:
                     f.write('\n') # Use '\n' consistently

        print(f"Generated {final_line_count} lines (target ~{target_lines}) in '{filename}'")
        print(f"Target violation probability: {violation_percentage:.1f}% (applied with variations)")
        # Report potential issues based on refined checks (with caveats)
        if unused_globals: print(f"NOTE: Potential unused global variables (F841 heuristic): {', '.join(unused_globals)}")
        if unused_funcs: print(f"NOTE: Potential unused functions (F841 heuristic): {', '.join(unused_funcs)}")
        if unused_imports_found: print(f"NOTE: Potential unused imports (F401 heuristic): {', '.join(unused_imports_found)}")
        print("\nRun flake8 (with plugins) to check for violations:")
        print(f"  flake8 --select=E,W,F,B,N,SIM,C4 {filename}")
        print("(Requires: pip install flake8 flake8-bugbear flake8-naming flake8-simplify flake8-comprehensions)")
        print("NOTE: The generated code aims for runnability but contains complex/misleading patterns.")
        print("      Some 'unused' warnings might be inaccurate due to complex usage patterns.")

    except IOError as e: print(f"Error writing file: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error during final processing/writing: {e}", file=sys.stderr)
        traceback.print_exc()

# --- Command Line Interface ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser( description="Generate runnable Python code with harder-to-fix violations (E,W,F,B,N,SIM,C4)." )
    parser.add_argument( "--lines", type=int, default=600, help="Target number of lines (approximate)." ) # Increased default
    parser.add_argument( "--percentage", type=float, default=85.0, help="Target probability (%%) for generating violations (influences complexity)." ) # Slightly lower default, complexity adds difficulty
    parser.add_argument( "--output", type=str, default="generated_harder_code.py", help="Output filename." ) # New default name
    args = parser.parse_args()
    if not (0 <= args.percentage <= 100): parser.error("Percentage must be 0-100.")
    if args.lines <= 0: parser.error("Lines must be positive.")

    generate_code(args.lines, args.percentage, args.output)

