# -*- coding: utf-8 -*-

"""
knowledge_graph_generator_and_merger_v2.py

Purpose:
Generates multiple fictional, structured Knowledge Graphs (KGs) centered around
unique protagonists, incorporating influences like socio-economic background,
historical context, and life turning points. Ensures temporal consistency.
For each KG, it saves the full KG data, the subgraph relevant to sentences,
visualizations for both, and the sentences themselves into structured
subdirectories. Finally, it merges all generated sentence lists from the run
into a single aggregated JSON file.

Design Philosophy: (Same as before)
Sociological Depth, Timeline Consistency, Capital Conversion/Field Conflict Narrative,
Archetype-Driven Generation, Protagonist-Centric Expansion, Controlled Randomness,
Structured Output (kg/, subkg/, viz/, subviz/, sentences/), NL Conversion with Focus,
Visualization, Aggregation, Modularity & Robustness.

Usage Instructions: (Same as before)
Examples: (Same as before)
Arguments: (Same as before)
Requirements: (Same as before)

Run:
python ./core/simulation/kg2text.py \
    --num-datasets 150 \
    --output-dir /mnt/data/zikai/longeval/data/kg2text/1k \
    --size 15 \
    --no-kg \
    --no-sentences \
    --no-viz
"""


import json
import random
import uuid
import argparse
import os
import sys
from faker import Faker
from collections import deque
from datetime import datetime, timedelta, date
import time
import re # For improved slug sanitization
import traceback # For printing detailed errors

# --- Configuration ---
DEFAULT_NUM_DATASETS = 10000 # Reduced for testing
DEFAULT_OUTPUT_DIR = "./data/kg2text" # Default local test output
DEFAULT_TARGET_NODE_COUNT_OPTIONS = [5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32]
MAX_EXPAND_PER_NODE = 50
MIN_EXPAND_PER_NODE = 1

DEFAULT_MAX_DISTANCE = 1
DEFAULT_VIZ_PROG = 'sfdp'
DEFAULT_VIZ_FORMAT = 'png'
CONNECT_TO_EXISTING_PROB = 0.35
CHARACTER_CENTRIC_BIAS = 2.5
MINIMUM_DESCRIPTION_PROB = 0.30
LIFESPAN_MIN_YEARS = 45
LIFESPAN_MAX_YEARS = 95

# --- Attempt to import visualization libraries ---
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False

try:
    import pygraphviz as pgv
    HAS_PYGRAPHVIZ = True
except ImportError:
    HAS_PYGRAPHVIZ = False

# --- Socio-Economic, Archetypes, Faker Initialization ---
SOCIO_ECONOMIC_BACKGROUNDS = {
    'Underprivileged': {'edu_boost': 0.6, 'found_boost': 0.3, 'invest_boost': 0.1, 'prestige_edu_prob': 0.1, 'high_status_job_prob': 0.15, 'base_influence': 0.7},
    'Working Class': {'edu_boost': 0.8, 'found_boost': 0.5, 'invest_boost': 0.3, 'prestige_edu_prob': 0.25, 'high_status_job_prob': 0.3, 'base_influence': 0.9},
    'Middle Class': {'edu_boost': 1.0, 'found_boost': 1.0, 'invest_boost': 1.0, 'prestige_edu_prob': 0.5, 'high_status_job_prob': 0.6, 'base_influence': 1.0},
    'Upper Middle Class': {'edu_boost': 1.2, 'found_boost': 1.3, 'invest_boost': 1.4, 'prestige_edu_prob': 0.7, 'high_status_job_prob': 0.75, 'base_influence': 1.1},
    'Upper Class': {'edu_boost': 1.5, 'found_boost': 1.8, 'invest_boost': 2.0, 'prestige_edu_prob': 0.9, 'high_status_job_prob': 0.9, 'base_influence': 1.3}
}
ARCHETYPES = {
'Scientist': {
'birth_range': (45, 70), 'common_jobs': ['Researcher', 'Professor', 'Physicist', 'Biologist', 'Chemist', 'Astronomer', 'Data Scientist', 'Lead Scientist', 'Inventor'],
'rel_boost': {'educated_at': 1.7, 'authored': 1.6, 'created': 0.9, 'participated_in': 1.4, 'influenced_by': 1.5, 'influenced': 1.4, 'worked_at': 1.3, 'member_of': 1.1, 'founded': 0.7, 'invested_in': 0.4, 'knows': 1.0, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.0, 'lived_in': 1.0, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Artist': {
'birth_range': (35, 60), 'common_jobs': ['Painter', 'Musician', 'Writer', 'Sculptor', 'Photographer', 'Designer', 'Actor', 'Director', 'Poet', 'Playwright'],
'rel_boost': {'created': 1.8, 'authored': 1.5, 'participated_in': 1.5, 'educated_at': 1.0, 'influenced_by': 1.4, 'influenced': 1.3, 'worked_at': 0.8, 'member_of': 1.2, 'founded': 0.7, 'invested_in': 0.5, 'knows': 1.2, 'spouse_of': 1.1, 'child_of': 1.0, 'parent_of': 1.0, 'lived_in': 1.1, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Entrepreneur': {
'birth_range': (40, 65), 'common_jobs': ['CEO', 'Founder', 'Investor', 'Business Owner', 'Consultant', 'Chief Technology Officer', 'Venture Capitalist', 'Board Member'],
'rel_boost': {'founded': 2.0, 'invested_in': 1.8, 'worked_at': 1.5, 'partnered_with': 1.6, 'knows': 1.4, 'participated_in': 1.2, 'educated_at': 1.0, 'authored': 0.7, 'created': 0.6, 'influenced_by': 1.0, 'influenced': 1.2, 'member_of': 1.0, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.0, 'lived_in': 1.0, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Politician': {
'birth_range': (50, 75), 'common_jobs': ['Senator', 'Minister', 'Mayor', 'Diplomat', 'Activist', 'Judge', 'Governor', 'Campaign Manager', 'Party Leader'],
'rel_boost': {'worked_at': 1.5, 'member_of': 1.7, 'participated_in': 1.8, 'knows': 1.6, 'influenced': 1.5, 'influenced_by': 1.2, 'authored': 0.9, 'educated_at': 1.2, 'founded': 0.8, 'created': 0.5, 'invested_in': 0.6, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.0, 'lived_in': 1.1, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Doctor_Healer': {
'birth_range': (40, 65), 'common_jobs': ['Doctor', 'Surgeon', 'Physician', 'Medical Researcher', 'Nurse Practitioner', 'Psychiatrist', 'Public Health Official'],
'rel_boost': {'educated_at': 1.8, 'worked_at': 1.7, 'participated_in': 1.3, 'knows': 1.2, 'authored': 1.3, 'created': 0.7, 'founded': 0.8, 'influenced_by': 1.3, 'influenced': 1.4, 'member_of': 1.2, 'invested_in': 0.5, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.0, 'lived_in': 1.0, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Lawyer_Judge': {
'birth_range': (45, 70), 'common_jobs': ['Lawyer', 'Attorney', 'Judge', 'Prosecutor', 'Legal Counsel', 'Advocate', 'Law Professor'],
'rel_boost': {'educated_at': 1.8, 'worked_at': 1.6, 'participated_in': 1.5, 'knows': 1.5, 'authored': 1.2, 'member_of': 1.3, 'influenced_by': 1.3, 'influenced': 1.4, 'founded': 0.9, 'created': 0.6, 'invested_in': 0.7, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.0, 'lived_in': 1.0, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Teacher_Educator': {
'birth_range': (30, 65), 'common_jobs': ['Teacher', 'Educator', 'School Principal', 'Professor (Teaching Focused)', 'Tutor', 'Curriculum Developer', 'Educational Administrator'],
'rel_boost': {'educated_at': 1.5, 'worked_at': 1.8, 'influenced': 1.7, 'influenced_by': 1.3, 'authored': 1.1, 'participated_in': 1.1, 'member_of': 1.2, 'knows': 1.3, 'created': 0.8, 'founded': 0.6, 'invested_in': 0.4, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.1, 'lived_in': 1.0, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Journalist_Reporter': {
'birth_range': (30, 60), 'common_jobs': ['Journalist', 'Reporter', 'Editor', 'Broadcaster', 'War Correspondent', 'Investigative Journalist', 'Columnist', 'Photojournalist'],
'rel_boost': {'participated_in': 1.9, 'authored': 1.7, 'worked_at': 1.4, 'knows': 1.6, 'lived_in': 1.3, 'influenced_by': 1.2, 'influenced': 1.4, 'educated_at': 1.1, 'created': 0.7, 'member_of': 1.0, 'founded': 0.6, 'invested_in': 0.5, 'spouse_of': 1.0, 'child_of': 1.0, 'parent_of': 1.0, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
},
'Explorer_Adventurer': {
'birth_range': (30, 55), 'common_jobs': ['Explorer', 'Adventurer', 'Mountaineer', 'Archaeologist (Field)', 'Cartographer', 'Sailor', 'Pilot (Expedition)', 'Naturalist'],
'rel_boost': {'participated_in': 2.0, 'lived_in': 1.6, 'authored': 1.4, 'knows': 1.2, 'influenced_by': 1.3, 'influenced': 1.1, 'created': 1.0, 'worked_at': 0.7, 'member_of': 1.3, 'educated_at': 0.9, 'founded': 0.6, 'invested_in': 0.4, 'spouse_of': 0.9, 'child_of': 1.0, 'parent_of': 0.9, 'born_in': 1.0, 'died_in': 1.0, 'grew_up_in': 1.0}
}
}
fake = Faker()

# --- KG Structure Definitions & Generation Functions ---
HISTORICAL_ERAS = {
    (1800, 1914): "Pre-WWI Era", (1914, 1945): "World Wars Era", (1946, 1965): "Post-War Boom",
    (1966, 1980): "Social Change Era", (1981, 2000): "Late 20th Century / Early Digital Age",
    (2001, 2015): "Post-9/11 & Web 2.0 Era", (2016, 2024): "Contemporary Era",
}
def get_historical_era(year):
    if not year: return "Unknown Era"
    try:
        year_int = int(year)
        for (start, end), era in HISTORICAL_ERAS.items():
            if start <= year_int <= end: return era
        if year_int < 1800: return "Early Modern Era"
        if year_int > 2024: return "Near Future / Contemporary Era"
    except (ValueError, TypeError): pass
    return "Unknown Era"
def get_era_context_description(era, event_type=None):
    descriptions = {
        "Pre-WWI Era": f"This event occurred during a period of industrialization and rising global tensions.",
        "World Wars Era": f"Taking place amidst global conflict, this {event_type or 'event'} reflects the turmoil of the World Wars era.",
        "Post-War Boom": f"This {event_type or 'occurrence'} happened during a time of significant economic growth and societal rebuilding.",
        "Social Change Era": f"Reflecting the significant social and political shifts of the time, this {event_type or 'event'} is characteristic of the Social Change Era.",
        "Late 20th Century / Early Digital Age": f"Occurring at the cusp of the digital revolution, this {event_type or 'development'} hints at the technological changes to come.",
        "Post-9/11 & Web 2.0 Era": f"This {event_type or 'event'} is situated in the context of increased globalization, security concerns, and the rise of social media.",
        "Contemporary Era": f"This recent {event_type or 'event'} reflects current global trends and challenges.",
        "Early Modern Era": "This event dates back to the Early Modern period.",
        "Unknown Era": "The historical context for this event is unclear."}
    return descriptions.get(era, descriptions["Unknown Era"])
RELATIONSHIP_MAP = {
'Person': [('born_in', 'Place', 10, ['Childhood'], {}), ('died_in', 'Place', 8, ['LateLife'], {}), ('grew_up_in', 'Place', 15, ['Childhood'], {'description': lambda: f"Spent formative years here, shaping their early outlook."}), ('lived_in', 'Place', 12, None, {'start_date': lambda p_age, p_by: safe_date_between_strict(p_by, p_age, min_rel_age=5, max_rel_age_factor=0.8), 'end_date': lambda p_age, p_by, start_date: safe_date_between_strict(p_by, p_age, min_rel_date=start_date, is_end_date=True) if random.random()<0.6 else None, 'reason': lambda: f"Moved here seeking {random.choice(['new opportunities', 'family reasons', 'educational pursuits', 'a different environment', 'political refuge'])}." if random.random()<0.4 else None}), ('educated_at', 'Organization', 20, ['Education'], {'degree': lambda: random.choice(['B.Sc.', 'M.A.', 'Ph.D.', 'Diploma', 'B.A.', 'M.F.A', 'JD', 'MD']), 'major': lambda: fake.bs().title(), 'graduation_year': lambda p_age, p_by: safe_year_strict(p_by, p_age, min_offset=18, max_offset=30), 'thesis_topic': lambda: f"Research exploring {fake.bs().title()}" if random.random()<0.25 else None}), ('worked_at', 'Organization', 25, ['EarlyCareer', 'MidCareer', 'LateLife'], {'role': lambda: fake.job(), 'start_date': lambda p_age, p_by: safe_date_between_strict(p_by, p_age, min_rel_age=18, max_rel_age_factor=0.9), 'end_date': lambda p_age, p_by, start_date: safe_date_between_strict(p_by, p_age, min_rel_date=start_date, is_end_date=True) if random.random()<0.6 else None, 'description': lambda: f"Played a key role in {fake.bs()} during their tenure." if random.random()<0.35 else None}), ('member_of', 'Organization', 15, ['Education', 'MidCareer', 'LateLife'], {'role': lambda: random.choice(['Member', 'Board Member', 'Fellow', 'Volunteer', 'Committee Chair', 'Trustee']), 'start_date': lambda p_age, p_by: safe_date_between_strict(p_by, p_age, min_rel_age=16)}), ('knows', 'Person', 20, None, {'relationship_type': lambda: random.choice(['Friend', 'Colleague', 'Mentor', 'Mentee', 'Rival', 'Acquaintance', 'Family Friend', 'Collaborator', 'Confidante'])}), ('spouse_of', 'Person', 10, ['EarlyCareer', 'MidCareer'], {'start_date': lambda p_age, p_by: safe_date_between_strict(p_by, p_age, min_rel_age=20, max_rel_age_factor=0.7), 'end_date': lambda p_age, p_by, start_date: safe_date_between_strict(p_by, p_age, min_rel_date=start_date, is_end_date=True) if random.random()<0.2 else None}), ('child_of', 'Person', 10, ['Childhood'], {}), ('parent_of', 'Person', 15, ['MidCareer', 'LateLife'], {}), ('influenced_by', 'Person', 8, ['Education', 'EarlyCareer'], {'description': lambda: f"Their work significantly shaped {fake.first_name()}'s intellectual trajectory."}), ('influenced', 'Person', 8, ['MidCareer', 'LateLife'], {'description': lambda: f"Became a notable influence on subsequent generations or peers."}), ('authored', 'Work', 20, ['EarlyCareer', 'MidCareer', 'LateLife'], {'year': lambda p_age, p_by: safe_year_strict(p_by, p_age, min_offset=20)}), ('created', 'Work', 15, ['MidCareer'], {'year': lambda p_age, p_by: safe_year_strict(p_by, p_age, min_offset=25), 'medium': lambda: random.choice(['Painting', 'Sculpture', 'Software', 'Methodology', 'Composition', 'Film', 'Theory', 'Policy Document'])}), ('participated_in', 'Event', 30, None, {'role': lambda: random.choice(['Attendee', 'Speaker', 'Organizer', 'Key Participant', 'Witness', 'Recipient', 'Panelist', 'Protester', 'Lead Negotiator', 'Victim', 'Beneficiary'])}), ('founded', 'Organization', 8, ['MidCareer'], {'year': lambda p_age, p_by: safe_year_strict(p_by, p_age, min_offset=22), 'description': lambda: f"Established with the goal of {fake.catch_phrase()}."}), ('invested_in', 'Organization', 5, ['MidCareer', 'LateLife'], {'amount': lambda: f"${random.randint(1,100)*1000 * random.choice([1, 1, 1, 10, 100])}", 'year': lambda p_age, p_by: safe_year_strict(p_by, p_age, min_offset=30)}), ('experienced', 'Event', 10, None, {'impact': lambda: random.choice(['Significant Career Shift', 'Personal Revelation', 'Financial Windfall', 'Major Setback', 'Shift in Worldview', 'Strengthened Resolve', 'Loss of Status'])}), ('rival_of', 'Person', 5, ['MidCareer', 'LateLife'], {'context': lambda: f"Competed within the {random.choice(['academic', 'business', 'political', 'artistic'])} field."})],
'Organization': [('located_in', 'Place', 15, None, {}), ('has_subsidiary', 'Organization', 5, None, {}), ('parent_organization', 'Organization', 5, None, {}), ('partnered_with', 'Organization', 8, None, {'project': lambda: f"Joint venture focused on {fake.bs()}"}), ('employs', 'Person', 9, None, {}), ('has_member', 'Person', 8, None, {}), ('founded_by', 'Person', 2, None, {}), ('competitor_of', 'Organization', 4, None, {'industry': lambda: fake.bs()})],
'Place': [('located_in', 'Place', 10, None, {}), ('capital_of', 'Place', 3, None, {}), ('historical_context', 'Event', 5, None, {'description': lambda: "Site of a significant historical event."})],
'Work': [('cites', 'Work', 10, None, {}), ('based_on', 'Work', 5, None, {}), ('critique_of', 'Work', 5, None, {}), ('related_to_event', 'Event', 8, None, {'description': lambda: "Directly addresses or documents this event."}), ('authored_by', 'Person', 10, None, {}), ('influenced_by_work', 'Work', 7, None, {})],
'Event': [('took_place_in', 'Place', 20, None, {}), ('part_of', 'Event', 10, None, {}), ('preceded_by', 'Event', 15, None, {}), ('followed_by', 'Event', 15, None, {}), ('caused', 'Event', 8, None, {'significance': lambda: random.choice(['Minor', 'Major', 'Transformative', 'Catalytic'])}), ('influenced_by_event', 'Event', 8, None, {}), ('related_work', 'Work', 10, None, {'description': lambda: "Inspired or documented by this work."}), ('participant', 'Person', 8, None, {}), ('led_to', 'Event', 6, None, {'nature': lambda: random.choice(['Further Research', 'Policy Change', 'Public Outcry', 'New Alliance', 'Increased Conflict'])})]}
def is_date_plausible(person_birth_year, person_death_year, event_date_or_year, min_age=0, max_age=None):
    if person_birth_year is None: return True
    try:
        birth_year = int(person_birth_year)
        death_year = int(person_death_year) if person_death_year is not None else None
        event_year = None
        if isinstance(event_date_or_year, (date, datetime)): event_year = event_date_or_year.year
        elif event_date_or_year is not None: event_year = int(event_date_or_year)
        if event_year is None: return True
        if event_year < birth_year + min_age: return False
        if death_year is not None and event_year > death_year: return False
        if max_age is not None and event_year > birth_year + max_age: return False
        return True
    except (ValueError, TypeError, OverflowError) as e: return False
def safe_date_between_strict(person_birth_year, person_age, min_rel_age=0, max_rel_age_factor=1.0, min_rel_date=None, is_end_date=False):
    if person_birth_year is None or person_age is None:
        try: return fake.date_between(start_date="-50y", end_date="now")
        except: return date.today() - timedelta(days=random.randint(365*5, 365*30))
    try:
        birth_year = int(person_birth_year)
        current_year = datetime.now().year
        lifespan = LIFESPAN_MAX_YEARS
        death_year_approx = min(birth_year + lifespan, current_year + 1)
        min_event_year = birth_year + min_rel_age
        max_event_year = min(birth_year + int(person_age * max_rel_age_factor), death_year_approx)
        max_event_year = max(min_event_year, max_event_year)
        min_year_for_date = max(1, min_event_year)
        max_year_for_date = max(1, max_event_year)
        start_date_limit = date(min_year_for_date, 1, 1)
        end_date_limit = date(max_year_for_date, 12, 31)
        if min_rel_date:
            min_rel_date_obj = None
            if isinstance(min_rel_date, str):
                try: min_rel_date_obj = date.fromisoformat(min_rel_date.split('T')[0])
                except ValueError: pass
            elif isinstance(min_rel_date, date): min_rel_date_obj = min_rel_date
            if min_rel_date_obj and min_rel_date_obj.year >= 1:
                if is_date_plausible(birth_year, death_year_approx, min_rel_date_obj):
                     min_date_for_comparison = min_rel_date_obj + timedelta(days=1 if is_end_date else 0)
                     start_date_limit = max(start_date_limit, min_date_for_comparison)
        if start_date_limit > end_date_limit:
            if min_rel_date_obj and min_rel_date_obj.year >= 1:
                 potential_end = min_rel_date_obj + timedelta(days=random.randint(30, 365*2))
                 if is_date_plausible(birth_year, death_year_approx, potential_end): return potential_end
            fallback_year = max(1, min(min_event_year + random.randint(0, 5), death_year_approx))
            return date(fallback_year, random.randint(1,12), random.randint(1,28))
        try: generated_date = fake.date_between(start_date=start_date_limit, end_date=end_date_limit)
        except OverflowError:
            safe_start = max(start_date_limit, date(1900, 1, 1))
            safe_end = min(end_date_limit, date(current_year, 12, 31))
            if safe_start > safe_end: safe_start = safe_end - timedelta(days=1)
            generated_date = fake.date_between(start_date=safe_start, end_date=safe_end)
        if not is_date_plausible(birth_year, death_year_approx, generated_date): return start_date_limit
        return generated_date
    except (ValueError, TypeError, OverflowError) as e:
        fallback_start_date = None
        fallback_end_date = date.today()
        if person_age and min_rel_age is not None:
             try: fallback_start_date = date.today() - timedelta(days=int(365.25 * (person_age - min_rel_age)))
             except: pass
        if min_rel_date:
            min_rel_date_obj = None
            if isinstance(min_rel_date, str):
                try: min_rel_date_obj = date.fromisoformat(min_rel_date.split('T')[0])
                except: pass
            elif isinstance(min_rel_date, date): min_rel_date_obj = min_rel_date
            if min_rel_date_obj: fallback_start_date = max(fallback_start_date, min_rel_date_obj) if fallback_start_date else min_rel_date_obj
        if not fallback_start_date or fallback_start_date > fallback_end_date : fallback_start_date = date.today() - timedelta(days=365*50)
        try: return fake.date_between(start_date=fallback_start_date, end_date=fallback_end_date)
        except: return date.today() - timedelta(days=random.randint(365*5, 365*30))
def safe_year_strict(person_birth_year, person_age, min_offset=0, max_offset=None):
    if person_birth_year is None or person_age is None:
        return str(random.randint(max(1800, datetime.now().year - 60), datetime.now().year))
    try:
        birth_year = int(person_birth_year)
        current_year = datetime.now().year
        lifespan = random.randint(LIFESPAN_MIN_YEARS, LIFESPAN_MAX_YEARS)
        death_year_approx = min(birth_year + lifespan, current_year + 1)
        min_event_year = birth_year + min_offset
        max_event_year = birth_year + max_offset if max_offset is not None else current_year
        max_event_year = min(max_event_year, death_year_approx)
        min_event_year = max(1, max(birth_year, min_event_year))
        max_event_year = max(min_event_year, max_event_year)
        if min_event_year > max_event_year: return str(min_event_year)
        return str(random.randint(min_event_year, max_event_year))
    except (ValueError, TypeError, OverflowError) as e:
        return str(random.randint(max(1800, datetime.now().year - 60), datetime.now().year))


# --- Attribute Generation ---
def generate_fictional_attributes(node_type, protagonist_birth_year=None, current_year=datetime.now().year, archetype_data=None, background_data=None, is_protagonist=False, existing_node_lookup=None):
    attributes = {}
    lifespan_years = random.randint(LIFESPAN_MIN_YEARS, LIFESPAN_MAX_YEARS)
    if random.random() < MINIMUM_DESCRIPTION_PROB:
        if node_type == 'Place':
            attributes['description'] = f"{random.choice(['Historic', 'Modern', 'Quiet', 'Bustling', 'Scenic', 'Industrial', 'Affluent', 'Developing'])} location."
        elif node_type == 'Organization':
            attributes['description'] = f"An organization focused on {fake.bs()}, known for its {random.choice(['innovative approach', 'traditional values', 'social impact', 'market dominance', 'controversial practices'])}."
        elif node_type == 'Work':
            attributes['description'] = f"A notable work concerning {fake.bs()}, considered {random.choice(['groundbreaking', 'influential', 'derivative', 'provocative', 'seminal'])} in its field."
        elif node_type == 'Event':
            attributes['description'] = f"A significant event related to {fake.bs()}, marking a {random.choice(['turning point', 'culmination', 'new beginning', 'period of crisis', 'moment of celebration'])}."

    if node_type == 'Person':
        attributes['name'] = fake.name()
        if is_protagonist or random.random() < 0.7:
             chosen_background = random.choices(list(SOCIO_ECONOMIC_BACKGROUNDS.keys()), weights=[0.1, 0.25, 0.35, 0.2, 0.1], k=1)[0]
             attributes['socioeconomic_background'] = chosen_background
             background_data = SOCIO_ECONOMIC_BACKGROUNDS[chosen_background]
        else:
             if background_data is None:
                 attributes['socioeconomic_background'] = 'Middle Class'
                 background_data = SOCIO_ECONOMIC_BACKGROUNDS['Middle Class']
             else:
                 inferred_bg = next((k for k, v in SOCIO_ECONOMIC_BACKGROUNDS.items() if v == background_data), 'Middle Class')
                 attributes['socioeconomic_background'] = inferred_bg

        birth_year = None
        if is_protagonist and archetype_data:
            min_age_rel, max_age_rel = archetype_data.get('birth_range', (40, 70))
            age_at_present = random.randint(min_age_rel, max_age_rel)
            birth_year = current_year - age_at_present
        elif protagonist_birth_year and not is_protagonist: # Bias related person's age
             max_age_diff = 20
             birth_year_offset = random.randint(-max_age_diff, max_age_diff)
             birth_year = protagonist_birth_year + birth_year_offset
        else:
             age_at_present = random.randint(25, 85)
             birth_year = current_year - age_at_present

        min_reasonable_birth_year = max(1800, current_year - LIFESPAN_MAX_YEARS - 20)
        max_reasonable_birth_year = current_year - 18
        birth_year = max(min_reasonable_birth_year, min(max_reasonable_birth_year, birth_year))
        attributes['birth_year'] = birth_year

        if birth_year:
            individual_lifespan = random.randint(LIFESPAN_MIN_YEARS, LIFESPAN_MAX_YEARS)
            potential_death_year = birth_year + individual_lifespan
            prob_deceased = 0.0
            if potential_death_year < current_year:
                 years_past_min_lifespan = max(0, current_year - (birth_year + LIFESPAN_MIN_YEARS))
                 prob_deceased = min(0.95, 0.1 + 0.85 * (years_past_min_lifespan / (LIFESPAN_MAX_YEARS - LIFESPAN_MIN_YEARS + 5)))
            if potential_death_year < current_year and random.random() < prob_deceased :
                 attributes['death_year'] = potential_death_year
            elif random.random() < 0.05 and (current_year - birth_year) > 30 and 'death_year' not in attributes:
                 early_death_age = random.randint(30, max(31, individual_lifespan - 5))
                 attributes['death_year'] = birth_year + early_death_age

        job = None
        high_status_jobs = ['CEO', 'Founder', 'Investor', 'Senator', 'Minister', 'Judge', 'Governor', 'Doctor', 'Surgeon', 'Lead Scientist', 'Professor', 'Chief Technology Officer', 'Director', 'Ambassador', 'Chancellor', 'General']
        is_high_status_attempt = False
        person_background_data = SOCIO_ECONOMIC_BACKGROUNDS.get(attributes.get('socioeconomic_background', 'Middle Class'))
        if person_background_data and random.random() < person_background_data['high_status_job_prob']:
            is_high_status_attempt = True

        if is_protagonist and archetype_data:
            possible_jobs = archetype_data.get('common_jobs', [fake.job()])
            if is_high_status_attempt:
                high_status_in_archetype = [j for j in possible_jobs if j in high_status_jobs]
                if high_status_in_archetype:
                    job = random.choice(high_status_in_archetype)
                else:
                    job = random.choice(possible_jobs)
            else:
                non_high_status_in_archetype = [j for j in possible_jobs if j not in high_status_jobs]
                if non_high_status_in_archetype:
                    job = random.choice(non_high_status_in_archetype)
                else:
                    job = random.choice(possible_jobs)
        else:
            if is_high_status_attempt and random.random() < 0.6:
                job = random.choice(high_status_jobs)
            else:
                job = fake.job()
        attributes['job'] = job

        if random.random() < 0.6: attributes['nationality'] = fake.country()
        if random.random() < 0.25:
            attributes['stated_motivation'] = random.choice([
                "Driven by intellectual curiosity.", "Sought to create lasting change.",
                "Focused on artistic expression.", "Aimed for financial independence.",
                "Committed to social justice.", "Valued community and connection.",
                "Motivated by personal ambition."
            ])

    elif node_type == 'Place':
        place_type = random.choice(['City', 'Country', 'Region', 'Building', 'Landmark', 'University Campus', 'Laboratory', 'Hospital', 'Museum', 'Theatre', 'District', 'Neighborhood'])
        name = f"Generic {place_type}" # Default name
        try:
            if place_type == 'City': name = fake.city()
            elif place_type == 'Country': name = fake.country()
            elif place_type == 'Region': name = fake.state()
            elif place_type == 'District': name = f"{fake.word().capitalize()} District"
            elif place_type == 'Neighborhood': name = f"{fake.street_name()} Neighborhood"
            elif place_type == 'Building': name = f"{fake.last_name()} {random.choice(['Tower', 'Building', 'Hall', 'Center', 'Complex', 'Institute'])}"
            elif place_type == 'Landmark': name = f"{fake.word().capitalize()} {random.choice(['Bridge', 'Square', 'Park', 'Monument', 'Plaza'])}"
            elif place_type == 'University Campus': name = f"{fake.city()} University Campus"
            elif place_type == 'Laboratory': name = f"The {fake.word().capitalize()} Research Laboratory"
            elif place_type == 'Hospital': name = f"{fake.city()} General Hospital" if random.random() < 0.5 else f"St. {fake.first_name()} Medical Center"
            elif place_type == 'Museum': name = f"Museum of {random.choice(['Modern Art', 'Natural History', 'Science and Industry', 'Cultural Heritage'])}"
            elif place_type == 'Theatre': name = f"The {fake.last_name()} Theatre"
            # else: name remains the default
        except Exception as e:
            # print(f"[WARN] Faker error generating place name ({place_type}): {e}. Using fallback.")
            name = f"Generic {place_type}" # Ensure fallback on error

        attributes['name'] = name
        attributes['place_type'] = place_type
        if random.random() < 0.2:
            attributes['dominant_era_feel'] = get_historical_era(random.randint(1850, 2000))

    elif node_type == 'Organization':
        org_type = random.choice(['Company', 'University', 'Research Institute', 'Foundation', 'Government Agency', 'Startup', 'Non-Profit', 'Political Party', 'Publisher', 'Museum', 'Hospital', 'School', 'Law Firm', 'News Agency', 'Think Tank', 'Trade Union'])
        name = f"Generic {org_type}" # Default name
        try:
            if org_type == 'Company': name = fake.company()
            elif org_type == 'University': name = f"{fake.city()} University" if random.random() < 0.7 else f"University of {fake.state()}"
            elif org_type == 'Research Institute': name = f"Institute for {fake.bs().title()}"
            elif org_type in ['Foundation', 'Non-Profit']: name = f"{fake.catch_phrase()} Foundation"
            elif org_type == 'Government Agency': name = f"Ministry of {fake.word().capitalize()}" if random.random() < 0.6 else f"{fake.city()} {random.choice(['Council', 'Department', 'Agency', 'Bureau'])}"
            elif org_type == 'Startup': name = f"{fake.word().capitalize()} Labs"
            elif org_type == 'Political Party': name = f"The {fake.word().capitalize()} Party"
            elif org_type == 'Publisher': name = f"{fake.last_name()} Press" if random.random() < 0.6 else f"{fake.city()} Publishing House"
            elif org_type == 'Museum': name = f"{fake.city()} Museum of {random.choice(['Art', 'History', 'Science'])}"
            elif org_type == 'Hospital': name = f"{fake.city()} General Hospital"
            elif org_type == 'School': name = f"{fake.city()} {random.choice(['High School', 'Elementary', 'Academy'])}"
            elif org_type == 'Law Firm': name = f"{fake.last_name()}, {fake.last_name()} & {fake.last_name()}" if random.random() < 0.5 else f"{fake.last_name()} Associates"
            elif org_type == 'News Agency': name = f"{fake.city()} {random.choice(['Times', 'Chronicle', 'Post'])}" if random.random() < 0.6 else f"{fake.country()} News Service"
            elif org_type == 'Think Tank': name = f"The {fake.word().capitalize()} Institute for Policy Studies"
            elif org_type == 'Trade Union': name = f"Union of {fake.bs().title()} Workers"
            # else: name remains the default
        except Exception as e:
            # print(f"[WARN] Faker error generating org name ({org_type}): {e}. Using fallback.")
            name = f"Generic {org_type}" # Ensure fallback on error

        attributes['name'] = name
        attributes['org_type'] = org_type
        if random.random() < 0.5:
            attributes['founded_year'] = str(random.randint(1800, current_year - 1))
        if random.random() < 0.4:
            attributes['mission'] = fake.catch_phrase()
        if org_type in ['Company', 'Startup']:
            attributes['industry'] = fake.bs()
        if org_type in ['University', 'Research Institute', 'Law Firm', 'Think Tank', 'Museum'] and random.random() < 0.3:
            attributes['prestige_level'] = random.choice(['High', 'Notable', 'Respected'])
        elif org_type in ['Company', 'Startup'] and random.random() < 0.2:
             attributes['market_position'] = random.choice(['Leader', 'Challenger', 'Niche Player', 'Incumbent'])

    elif node_type == 'Work':
        work_type = random.choice(['Book', 'Article', 'Painting', 'Theory', 'Invention', 'Composition', 'Software', 'Patent', 'Thesis', 'Film', 'Sculpture', 'Play', 'Photograph', 'Map', 'Legal Document', 'Speech', 'Manifesto', 'Policy Paper'])
        name = f"Generic {work_type}" # Default name
        try:
            common_prefix = ["The", "A Study of", "Reflections on", "Analysis of", "Notes Towards a", "Manifesto on", "Policy Framework for"]
            common_suffix = ["Chronicles", 'Manifesto', 'Methodology', 'Framework', 'Principles', 'Experiment', 'Case Study', 'Impact Assessment']
            if work_type == 'Book': name = f"{random.choice(common_prefix)} {fake.bs().title()}" + (f" {random.choice(common_suffix)}" if random.random() > 0.7 else "")
            elif work_type == 'Article': name = f"On the Nature of {fake.bs().title()}"
            elif work_type in ['Painting', 'Sculpture', 'Photograph']: name = f"{fake.color_name().capitalize()} {fake.word().capitalize()} No. {random.randint(1,5)}"
            elif work_type == 'Theory': name = f"The Theory of {fake.bs().title()}"
            elif work_type in ['Invention', 'Patent']: name = f"The {fake.word().capitalize()} Device"
            elif work_type == 'Composition': name = f"{random.choice(['Symphony', 'Concerto', 'Quartet', 'Sonata'])} No. {random.randint(1, 9)}"
            elif work_type == 'Software': name = f"{fake.word().capitalize()} Suite"
            elif work_type == 'Thesis': name = f"A Thesis on {fake.bs().title()}"
            elif work_type == 'Film': name = f"{fake.catch_phrase().title()}: The Movie"
            elif work_type == 'Play': name = f"The {fake.word().capitalize()} {random.choice(['Tragedy', 'Comedy', 'Affair'])}"
            elif work_type == 'Map': name = f"Map of the {fake.word().capitalize()} Region"
            elif work_type == 'Legal Document': name = f"The {fake.last_name()} Brief" if random.random() < 0.5 else f"Ruling on Case #{random.randint(100,999)}"
            elif work_type == 'Speech': name = f"Address on {fake.bs()}"
            elif work_type == 'Manifesto': name = f"A Manifesto for {fake.bs().title()}"
            elif work_type == 'Policy Paper': name = f"Policy Recommendations Regarding {fake.bs()}"
            else: name = f"{work_type} related to {fake.bs()}" # Fallback if type not matched

            if name: name = name.replace(" Of ", " of ").replace(" The ", " the ").replace(" A ", " a ")
            else: name = f"{work_type} related to {fake.bs()}" # Ensure assigned if somehow empty
        except Exception as e:
            # print(f"[WARN] Faker error generating work name ({work_type}): {e}. Using fallback.")
            name = f"Generic {work_type}" # Ensure fallback on error

        attributes['name'] = name
        attributes['work_type'] = work_type
        if random.random() < 0.8:
             attributes['publication_year'] = str(random.randint(1800, current_year))
        if work_type in ['Book', 'Composition', 'Painting', 'Film', 'Play']:
            attributes['genre'] = fake.word()
        if random.random() < 0.3:
            attributes['reception'] = random.choice(['Widely Acclaimed', 'Controversial', 'Influential in Niche', 'Largely Ignored', 'Critically Panned', 'Landmark Achievement'])

    elif node_type == 'Event':
        event_type = random.choice([
            'Conference', 'Discovery', 'Publication', 'Exhibition', 'Conflict',
            'Political Change', 'Personal Milestone', 'Accident', 'Scandal',
            'Award Ceremony', 'Election', 'Treaty Signing', 'Protest', 'Lecture',
            'Debate', 'Trial', 'Expedition', 'Festival', 'Launch',
            'Turning Point: Opportunity', 'Turning Point: Setback',
            'Social Movement Peak', 'Economic Crisis', 'Technological Breakthrough'
        ])
        year_str = str(random.randint(1800, current_year))
        name = f"Generic {event_type} ({year_str})" # Default name
        try:
            if event_type == 'Conference': name = f"The {year_str} {fake.word().capitalize()} Summit on {fake.bs()}"
            elif event_type == 'Discovery': name = f"Discovery of the {fake.word().capitalize()} Effect ({year_str})"
            elif event_type == 'Publication': name = f"Major Publication Released ({year_str})"
            elif event_type == 'Exhibition': name = f"{fake.city()} Art Exhibition ({year_str})"
            elif event_type == 'Conflict': name = f"The {fake.city()} {random.choice(['Uprising', 'Accord', 'Incident', 'Crisis', 'Struggle'])} ({year_str})"
            elif event_type in ['Political Change', 'Election', 'Treaty Signing']: name = f"The {fake.country()} {event_type} of {year_str}"
            elif event_type == 'Personal Milestone': name = f"{random.choice(['Marriage', 'Birth of Child', 'Graduation', 'Retirement', 'Major Promotion'])} ({year_str})"
            elif event_type == 'Accident': name = f"The {fake.word()} Accident ({year_str})"
            elif event_type == 'Scandal': name = f"The {fake.company_suffix()} Scandal ({year_str})"
            elif event_type == 'Award Ceremony': name = f"The {fake.word().capitalize()} Prize Ceremony ({year_str})"
            elif event_type == 'Protest': name = f"{fake.city()} {random.choice(['Protests', 'March', 'Sit-in', 'Uprising'])} ({year_str})"
            elif event_type == 'Lecture': name = f"Lecture on {fake.bs()} ({year_str})"
            elif event_type == 'Debate': name = f"The Great {fake.word().capitalize()} Debate ({year_str})"
            elif event_type == 'Trial': name = f"The Trial of {fake.last_name()} ({year_str})"
            elif event_type == 'Expedition': name = f"The {fake.word().capitalize()} Expedition ({year_str})"
            elif event_type == 'Festival': name = f"{fake.city()} {random.choice(['Film', 'Music', 'Arts', 'Ideas'])} Festival ({year_str})"
            elif event_type == 'Launch': name = f"Launch of the {fake.word().capitalize()} Project ({year_str})"
            elif event_type == 'Turning Point: Opportunity': name = f"Significant Opportunity Emerges ({year_str})"
            elif event_type == 'Turning Point: Setback': name = f"Major Setback Encountered ({year_str})"
            elif event_type == 'Social Movement Peak': name = f"Height of the {fake.word().capitalize()} Movement ({year_str})"
            elif event_type == 'Economic Crisis': name = f"The {year_str} Economic Downturn"
            elif event_type == 'Technological Breakthrough': name = f"Breakthrough in {fake.bs().title()} ({year_str})"
            # else: name remains the default
        except Exception as e:
             # print(f"[WARN] Faker error generating event name ({event_type}): {e}. Using fallback.")
             name = f"Generic {event_type} ({year_str})" # Ensure fallback on error

        attributes['name'] = name
        attributes['event_type'] = event_type
        try:
             attributes['year'] = int(year_str)
             attributes['historical_era'] = get_historical_era(attributes['year'])
             if random.random() < 0.4:
                 attributes['context_description'] = get_era_context_description(attributes['historical_era'], event_type)
        except (ValueError, TypeError):
             attributes['year'] = current_year - random.randint(1, 10)
             attributes['historical_era'] = get_historical_era(attributes['year'])

        if random.random() < 0.5: attributes['month'] = random.randint(1, 12)
        if attributes.get('month') and random.random() < 0.5: attributes['day'] = random.randint(1, 28)
        if random.random() < 0.4: attributes['outcome'] = random.choice(['Success', 'Failure', 'Mixed', 'Ongoing', 'Controversial', 'Unclear', 'Resolved', 'Escalated'])
        if random.random() < 0.5: attributes['significance'] = random.choice(['Low', 'Medium', 'High', 'Turning Point', 'Local', 'National', 'Global', 'Field-Specific'])

    if 'name' not in attributes or not attributes['name']:
        attributes['name'] = f"Unnamed {node_type}_{uuid.uuid4().hex[:4]}"
    return attributes

# --- Get Node Distances ---
def get_node_distances(protagonist_id, edges, node_lookup):
    if not protagonist_id or protagonist_id not in node_lookup:
        return {}
    distances = {protagonist_id: 0}
    queue = deque([protagonist_id])
    visited = {protagonist_id}
    adjacency = {}
    for edge in edges:
        source = edge.get('source')
        target = edge.get('target')
        if source and target and source in node_lookup and target in node_lookup:
            adjacency.setdefault(source, []).append(target)
            adjacency.setdefault(target, []).append(source) # Treat as undirected

    processed_nodes_count = 0
    max_nodes_to_process = len(node_lookup) * 3 # Safety limit

    while queue and processed_nodes_count < max_nodes_to_process:
        current_id = queue.popleft()
        processed_nodes_count += 1
        current_dist = distances[current_id]
        for neighbor in adjacency.get(current_id, []):
            if neighbor and neighbor in node_lookup and neighbor not in visited:
                visited.add(neighbor)
                distances[neighbor] = current_dist + 1
                queue.append(neighbor)
    return distances

# --- Get Life Phase ---
def get_life_phase(birth_year, current_event_year):
    if birth_year is None or current_event_year is None:
        return random.choice(['EarlyCareer', 'MidCareer'])
    try:
        age = int(current_event_year) - int(birth_year)
        if age < 0:
            return 'Childhood'
        elif age < 12:
            return 'Childhood'
        elif age < 22:
            return 'Education'
        elif age < 40:
            return 'EarlyCareer'
        elif age < 65:
            return 'MidCareer'
        else: # age >= 65
            return 'LateLife'
    except (ValueError, TypeError):
        return random.choice(['EarlyCareer', 'MidCareer'])

# --- Main KG Generation ---
def generate_fictional_kg_rich(character_name, archetype_name=None, target_node_count=DEFAULT_TARGET_NODE_COUNT_OPTIONS[0]):
    nodes = []
    edges = []
    node_lookup = {}
    protagonist_id = None
    current_year = datetime.now().year

    chosen_archetype_name = archetype_name
    if not chosen_archetype_name or chosen_archetype_name not in ARCHETYPES:
        chosen_archetype_name = random.choice(list(ARCHETYPES.keys()))
    archetype_data = ARCHETYPES[chosen_archetype_name]

    char_attributes = generate_fictional_attributes(
        'Person', current_year=current_year, archetype_data=archetype_data,
        background_data=None, is_protagonist=True, existing_node_lookup=node_lookup
    )
    char_attributes['name'] = character_name
    protagonist_birth_year = char_attributes.get('birth_year')
    protagonist_death_year = char_attributes.get('death_year')
    protagonist_background = char_attributes.get('socioeconomic_background', 'Middle Class')
    background_data = SOCIO_ECONOMIC_BACKGROUNDS[protagonist_background]

    char_id = str(uuid.uuid4())
    protagonist_id = char_id
    char_node = {'id': char_id, 'type': 'Person', 'attributes': char_attributes}
    nodes.append(char_node)
    node_lookup[char_id] = char_node

    queue = deque([char_id])
    processed_for_expansion = set()
    nodes_in_queue = {char_id}
    expansion_iterations = 0
    max_total_iterations = target_node_count * MAX_EXPAND_PER_NODE * 10

    while queue and len(nodes) < target_node_count and expansion_iterations < max_total_iterations:
        expansion_iterations += 1

        current_node_id = queue.popleft()
        current_node = node_lookup.get(current_node_id)

        if not current_node or current_node_id in processed_for_expansion:
            continue

        processed_for_expansion.add(current_node_id)
        current_node_type = current_node.get('type')
        current_node_attrs = current_node.get('attributes', {})
        if not current_node_type:
            continue

        current_is_person = (current_node_type == 'Person')
        current_birth_year = current_node_attrs.get('birth_year') if current_is_person else None
        current_death_year = current_node_attrs.get('death_year') if current_is_person else None
        current_age_approx = (current_year - current_birth_year) if current_birth_year else None
        current_background_data = SOCIO_ECONOMIC_BACKGROUNDS.get(current_node_attrs.get('socioeconomic_background')) if current_is_person else None

        distances = get_node_distances(protagonist_id, edges, node_lookup)
        distance_from_protagonist = distances.get(current_node_id, 99)
        bias_factor = max(1.0, CHARACTER_CENTRIC_BIAS / (distance_from_protagonist + 1.0))
        base_expand = random.randint(MIN_EXPAND_PER_NODE, MAX_EXPAND_PER_NODE)
        num_relations_to_add = min(MAX_EXPAND_PER_NODE, int(base_expand * bias_factor))
        num_relations_to_add = max(MIN_EXPAND_PER_NODE if len(nodes) < target_node_count else 0, num_relations_to_add)
        current_connect_prob = min(0.9, CONNECT_TO_EXISTING_PROB * bias_factor)

        possible_relations_defs = RELATIONSHIP_MAP.get(current_node_type, [])
        valid_relations_for_choice = []
        weights = []

        rel_year_approx = current_year - random.randint(5, 30)
        if current_is_person and current_birth_year:
             min_active_age = 16
             max_active_age = LIFESPAN_MAX_YEARS
             person_lifespan = LIFESPAN_MAX_YEARS
             if current_death_year:
                  if current_death_year > current_birth_year:
                      person_lifespan = current_death_year - current_birth_year
                      max_active_age = min(max_active_age, person_lifespan)
                  else:
                      current_death_year = None # Invalidate bad death year
             if current_age_approx is not None and current_death_year is None:
                  max_active_age = min(max_active_age, current_age_approx)
             max_active_age = max(min_active_age, max_active_age)

             if max_active_age > min_active_age:
                 try:
                      rel_age = random.randint(min_active_age, max_active_age)
                      rel_year_approx = current_birth_year + rel_age
                 except ValueError:
                      rel_year_approx = current_birth_year + min_active_age
             else:
                  rel_year_approx = current_birth_year + min_active_age + random.randint(0,2)
             rel_year_approx = max(1, min(rel_year_approx, current_year))

        life_phase = get_life_phase(current_birth_year, rel_year_approx) if current_is_person else None
        arch_boost_map = archetype_data.get('rel_boost', {}) if current_node_id == protagonist_id else {}
        bg_boost_map = {}
        if current_background_data:
            bg_boost_map['educated_at'] = current_background_data.get('edu_boost', 1.0)
            bg_boost_map['founded'] = current_background_data.get('found_boost', 1.0)
            bg_boost_map['invested_in'] = current_background_data.get('invest_boost', 1.0)
            bg_boost_map['influenced'] = current_background_data.get('base_influence', 1.0)
            high_status_jobs_list = ['CEO', 'Founder', 'Investor', 'Senator', 'Minister', 'Judge', 'Governor', 'Doctor', 'Surgeon', 'Lead Scientist', 'Professor', 'Chief Technology Officer', 'Director', 'Ambassador', 'Chancellor', 'General']
            if current_node_attrs.get('job') in high_status_jobs_list:
                 bg_boost_map['worked_at'] = current_background_data.get('base_influence', 1.0) * 1.1

        for rel_def in possible_relations_defs:
            if len(rel_def) < 3:
                continue # Skip malformed definitions
            rel_name = rel_def[0]
            target_type = rel_def[1]
            base_weight = rel_def[2]
            rel_phases = rel_def[3] if len(rel_def) > 3 else None

            phase_ok = True
            if current_is_person and rel_phases is not None:
                if not life_phase or life_phase not in rel_phases:
                    phase_ok = False

            if phase_ok:
                arch_boost = arch_boost_map.get(rel_name, 1.0)
                bg_boost = bg_boost_map.get(rel_name, 1.0)
                adjusted_weight = max(0.05, base_weight * arch_boost * bg_boost)

                if current_is_person and rel_name == 'died_in' and current_death_year is None:
                    adjusted_weight = 0.0
                if rel_name == 'born_in' and current_is_person:
                    adjusted_weight *= 0.1
                if current_is_person and rel_name == 'child_of' and current_age_approx and current_age_approx > 70:
                    adjusted_weight *= 0.05

                if adjusted_weight > 0:
                    valid_relations_for_choice.append(rel_def)
                    weights.append(adjusted_weight)

        if not valid_relations_for_choice:
            continue

        added_count = 0
        attempts = 0
        max_attempts = num_relations_to_add * 5

        while added_count < num_relations_to_add and len(nodes) < target_node_count and attempts < max_attempts:
            attempts += 1
            chosen_rel_def = None
            if weights and len(weights) == len(valid_relations_for_choice) and sum(weights) > 0:
                 try:
                     chosen_rel_def = random.choices(valid_relations_for_choice, weights=weights, k=1)[0]
                 except ValueError:
                     pass # Handle potential errors if weights are invalid
            elif valid_relations_for_choice: # Fallback to random choice if weights failed
                chosen_rel_def = random.choice(valid_relations_for_choice)

            if not chosen_rel_def:
                continue

            rel_name = chosen_rel_def[0]
            target_node_type = chosen_rel_def[1]
            attr_generators = chosen_rel_def[4] if len(chosen_rel_def) > 4 and isinstance(chosen_rel_def[4], dict) else {}

            target_node_id = None
            target_node_is_new = False
            target_node = None
            created_new_node = False

            if random.random() < current_connect_prob:
                potential_targets = [
                    n for n_id, n in node_lookup.items()
                    if n.get('type') == target_node_type and n_id != current_node_id
                ]
                if rel_name in ['parent_of', 'child_of'] and current_birth_year:
                    potential_targets = [
                        n for n in potential_targets if n.get('type')=='Person'
                        and abs(n.get('attributes', {}).get('birth_year', current_birth_year + 100) - current_birth_year) < 50
                    ]
                if rel_name == 'spouse_of' and current_birth_year:
                    potential_targets = [
                        n for n in potential_targets if n.get('type')=='Person'
                        and abs(n.get('attributes', {}).get('birth_year', current_birth_year + 100) - current_birth_year) < 30
                    ]
                if potential_targets:
                    target_node = random.choice(potential_targets)
                    target_node_id = target_node.get('id')

            if target_node_id is None and len(nodes) < target_node_count:
                 new_node_id = str(uuid.uuid4())
                 reference_birth_year_for_new_node = current_birth_year if current_is_person else protagonist_birth_year
                 new_node_attributes = generate_fictional_attributes(
                     target_node_type,
                     protagonist_birth_year=reference_birth_year_for_new_node,
                     current_year=current_year,
                     background_data=current_background_data if target_node_type == 'Person' else None,
                     existing_node_lookup=node_lookup
                 )
                 new_name = new_node_attributes.get('name')
                 is_duplicate = False
                 if new_name and target_node_type in ['Person', 'Organization', 'Work', 'Place']:
                     is_duplicate = any(
                         n.get('attributes', {}).get('name') == new_name and n.get('type') == target_node_type
                         for n_id, n in node_lookup.items() if n_id != current_node_id
                     )
                 if is_duplicate:
                     attempts += 2
                     continue

                 new_node = {'id': new_node_id, 'type': target_node_type, 'attributes': new_node_attributes}
                 nodes.append(new_node)
                 node_lookup[new_node_id] = new_node
                 target_node_id = new_node_id
                 target_node_is_new = True
                 target_node = new_node
                 created_new_node = True

            if not target_node_id or not target_node:
                continue

            is_self_loop = (current_node_id == target_node_id)
            symmetrical_rels = {'knows', 'spouse_of', 'partnered_with', 'rival_of', 'competitor_of'}
            is_duplicate_edge = False
            for e in edges:
                 s = e.get('source')
                 t = e.get('target')
                 r = e.get('relation')
                 if s == current_node_id and t == target_node_id and r == rel_name:
                     is_duplicate_edge = True
                     break
                 if rel_name in symmetrical_rels and s == target_node_id and t == current_node_id and r == rel_name:
                     is_duplicate_edge = True
                     break

            if not is_duplicate_edge: # Check other duplicates only if not already found
                if rel_name == 'child_of':
                    if any(e.get('source') == target_node_id and e.get('target') == current_node_id and e.get('relation') == 'child_of' for e in edges):
                        is_duplicate_edge = True
                    if any(e.get('source') == current_node_id and e.get('target') == target_node_id and e.get('relation') == 'parent_of' for e in edges):
                        is_duplicate_edge = True
                elif rel_name == 'parent_of':
                    if any(e.get('source') == target_node_id and e.get('target') == current_node_id and e.get('relation') == 'parent_of' for e in edges):
                        is_duplicate_edge = True
                    if any(e.get('source') == current_node_id and e.get('target') == target_node_id and e.get('relation') == 'child_of' for e in edges):
                        is_duplicate_edge = True

            if not is_duplicate_edge: # Check chronological only if not already duplicate
                target_birth_year = target_node.get('attributes', {}).get('birth_year')
                if current_is_person and current_birth_year and target_node.get('type') == 'Person' and target_birth_year:
                     min_parenting_age_diff = 15
                     if rel_name == 'child_of' and target_birth_year >= current_birth_year - min_parenting_age_diff:
                         is_duplicate_edge = True # Treat as invalid
                     elif rel_name == 'parent_of' and current_birth_year >= target_birth_year - min_parenting_age_diff:
                         is_duplicate_edge = True # Treat as invalid

            if not is_self_loop and not is_duplicate_edge:
                edge_attributes = {}
                valid_edge = True
                target_is_person = (target_node.get('type') == 'Person')
                target_birth_year = target_node.get('attributes', {}).get('birth_year') if target_is_person else None
                target_death_year = target_node.get('attributes', {}).get('death_year') if target_is_person else None
                generated_values = {}
                sorted_attr_keys = list(attr_generators.keys())
                if 'start_date' in sorted_attr_keys:
                    sorted_attr_keys.remove('start_date')
                    sorted_attr_keys.insert(0, 'start_date')

                for attr_name in sorted_attr_keys:
                    generator = attr_generators[attr_name]
                    if not callable(generator):
                        continue
                    try:
                        param_names = generator.__code__.co_varnames[:generator.__code__.co_argcount]
                        gen_args = []
                        if 'p_age' in param_names: gen_args.append(current_age_approx if current_age_approx is not None else 35)
                        if 'p_by' in param_names: gen_args.append(current_birth_year)
                        if attr_name == 'end_date' and 'start_date' in param_names: gen_args.append(generated_values.get('start_date'))

                        generated_value = None
                        if len(gen_args) == len(param_names):
                            generated_value = generator(*gen_args)
                        elif not param_names:
                            generated_value = generator()
                        else:
                            # print(f"[WARN] Arg mismatch for {attr_name} generator. Skipping.")
                            valid_edge = False
                            break # Stop processing attrs for this edge if generator fails

                        generated_values[attr_name] = generated_value
                        attr_value = generated_value

                        if attr_value is not None:
                            if attr_name in ['year', 'graduation_year']:
                                year_val = attr_value
                                if current_is_person and not is_date_plausible(current_birth_year, current_death_year, year_val, min_age=5):
                                    valid_edge = False
                                    break
                                if target_is_person and not is_date_plausible(target_birth_year, target_death_year, year_val, min_age=5):
                                    valid_edge = False
                                    break
                                generated_values[attr_name] = str(year_val)
                            elif attr_name in ['start_date', 'end_date']:
                                 date_val = attr_value
                                 if isinstance(date_val, (datetime, date)):
                                      if current_is_person and not is_date_plausible(current_birth_year, current_death_year, date_val):
                                          valid_edge = False
                                          break
                                      if target_is_person and not is_date_plausible(target_birth_year, target_death_year, date_val):
                                          valid_edge = False
                                          break
                                      generated_values[attr_name] = date_val.isoformat()
                                 elif date_val is not None:
                                     generated_values[attr_name] = str(date_val)
                    except Exception as e:
                        # print(f"[WARN] Error generating edge attribute '{attr_name}': {e}")
                        generated_values[attr_name] = None
                        if attr_name in ['year', 'start_date', 'graduation_year']:
                            valid_edge = False
                            break # Stop if critical attribute fails

                if not valid_edge:
                    attempts += 1
                    continue # Skip adding this edge

                edge_attributes = generated_values
                start_dt_str = edge_attributes.get('start_date')
                end_dt_str = edge_attributes.get('end_date')
                if start_dt_str and end_dt_str:
                    try:
                        start_date_obj = date.fromisoformat(start_dt_str.split('T')[0])
                        end_date_obj = date.fromisoformat(end_dt_str.split('T')[0])
                        if end_date_obj < start_date_obj:
                            edge_attributes['end_date'] = None
                    except (TypeError, ValueError, IndexError):
                         pass

                edge_id = str(uuid.uuid4())
                edge = {
                    'id': edge_id, 'source': current_node_id, 'target': target_node_id,
                    'relation': rel_name, 'attributes': edge_attributes
                }
                edges.append(edge)
                added_count += 1

                if target_node_is_new and target_node_id not in processed_for_expansion and target_node_id not in nodes_in_queue :
                    if len(nodes) < target_node_count:
                        queue.append(target_node_id)
                        nodes_in_queue.add(target_node_id)

                inverse_map = {
                    'child_of': 'parent_of', 'parent_of': 'child_of', 'worked_at': 'employs', 'employs': 'worked_at',
                    'member_of': 'has_member', 'has_member': 'member_of', 'influenced_by': 'influenced', 'influenced': 'influenced_by',
                    'founded': 'founded_by', 'founded_by': 'founded', 'authored': 'authored_by', 'authored_by': 'authored',
                    'created': 'created_by', 'participated_in': 'participant', 'participant': 'participated_in'
                }
                if rel_name in inverse_map:
                    inverse_rel_name = inverse_map[rel_name]
                    target_type = target_node.get('type')
                    is_inverse_defined = False
                    if target_type and target_type in RELATIONSHIP_MAP:
                        is_inverse_defined = any(r[0] == inverse_rel_name for r in RELATIONSHIP_MAP[target_type])
                    if is_inverse_defined:
                        is_inverse_present = any(
                            e.get('source') == target_node_id and e.get('target') == current_node_id and e.get('relation') == inverse_rel_name
                            for e in edges
                        )
                        if not is_inverse_present:
                            edges.append({
                                'id': str(uuid.uuid4()), 'source': target_node_id, 'target': current_node_id,
                                'relation': inverse_rel_name, 'attributes': {}
                            })

    if expansion_iterations >= max_total_iterations:
        print(f"[WARN] KG generation reached max iterations ({max_total_iterations}). Graph size might be smaller than target.")
    return {'nodes': nodes, 'edges': edges}


# --- Natural Language Conversion ---
RELATION_TEMPLATES = {
    'born_in': "{subj} was born in {obj}.", 'died_in': "{subj} passed away in {obj}.", 'grew_up_in': "{subj} spent their formative years growing up in {obj}.", 'lived_in': "{subj} resided in {obj}.", 'educated_at': "{subj} received education at {obj}.", 'worked_at': "{subj} was employed by {obj}.", 'member_of': "{subj} held membership in {obj}.", 'knows': "{subj} had a connection with {obj}.", 'spouse_of': "{subj} was married to {obj}.", 'child_of': "{subj} was the child of {obj}.", 'parent_of': "{subj} was the parent of {obj}.", 'influenced_by': "{subj}'s path was influenced by {obj}.", 'influenced': "{subj} exerted influence on {obj}.", 'authored': "{subj} authored the work '{obj}'.", 'created': "{subj} created '{obj}'.", 'participated_in': "{subj} was involved in the event '{obj}'.", 'founded': "{subj} established the organization {obj}.", 'invested_in': "{subj} made investments in {obj}.", 'located_in': "{subj} is situated within {obj}.", 'has_subsidiary': "{subj} operates the subsidiary {obj}.", 'parent_organization': "{subj} is under the umbrella of {obj}.", 'partnered_with': "{subj} formed a partnership with {obj}.", 'capital_of': "{subj} serves as the capital for {obj}.", 'cites': "The work '{subj}' references '{obj}'.", 'based_on': "'{subj}' draws upon the work '{obj}'.", 'critique_of': "'{subj}' offers a critique of '{obj}'.", 'related_to_event': "The work '{subj}' is connected to the event '{obj}'.", 'took_place_in': "The event '{subj}' occurred in {obj}.", 'part_of': "'{subj}' was a component of the larger event '{obj}'.", 'preceded_by': "The event '{subj}' followed '{obj}'.", 'followed_by': "The event '{subj}' preceded '{obj}'.", 'caused': "The event '{subj}' is considered a cause of '{obj}'.", 'influenced_by_event': "{subj} was shaped by the event '{obj}'.", 'related_work': "The event '{subj}' has ties to the work '{obj}'.", 'employs': "{subj} provided employment for {obj}.", 'has_member': "{subj} counted {obj} among its members.", 'founded_by': "{subj} owes its founding to {obj}.", 'authored_by': "'{obj}' was written by {subj}.", 'participant': "{obj} was a participant in the event '{subj}'.", 'created_by': "'{obj}' was created by {subj}.", 'experienced': "{subj} went through the event '{obj}'.", 'rival_of': "{subj} was a known rival of {obj}.", 'competitor_of': "{subj} was a competitor against {obj}.", 'historical_context': "{subj} was the location for the historical event '{obj}'.", 'influenced_by_work': "'{subj}' shows influence from the earlier work '{obj}'.", 'led_to': "The event '{subj}' contributed to or resulted in '{obj}'.", 'default': "{subj} has an unspecified relationship ('{rel}') with {obj}."
}
NODE_ATTR_TEMPLATES = {
    'name': None, 'description': "{subj}: {val}", 'summary': "{subj} summary: {val}", 'birth_year': "{subj} was born around {val}.", 'death_year': "{subj} died around {val}.", 'job': "{subj}'s occupation was {val}.", 'nationality': "{subj} held {val} nationality.", 'place_type': "{subj} is identified as a {val}.", 'org_type': "{subj} functions as a {val}.", 'founded_year': "{subj} was established around {val}.", 'mission': "{subj}'s stated mission involves '{val}'.", 'industry': "{subj} operates within the {val} industry.", 'work_type': "'{subj}' is classified as a {val}.", 'publication_year': "'{subj}' saw publication around {val}.", 'genre': "The genre associated with '{subj}' is {val}.", 'event_type': "'{subj}' is categorized as a {val} event.", 'year': "The event '{subj}' is dated around {val}.", 'month': None, 'day': None, 'outcome': "A noted outcome of '{subj}' was {val}.", 'significance': "The event '{subj}' is considered to have {val} significance.", 'socioeconomic_background': "{subj} came from a {val} background.", 'stated_motivation': "A driving motivation for {subj} was: {val}", 'dominant_era_feel': "{subj} has a distinct {val} atmosphere.", 'prestige_level': "{subj} holds a {val} level of prestige.", 'market_position': "{subj} occupies a {val} market position.", 'reception': "The work '{subj}' received a reception described as: {val}.", 'historical_era': "The event '{subj}' belongs to the {val}.", 'context_description': "Context for '{subj}': {val}", 'default': "Regarding {subj}, the attribute '{key}' is noted as {val}."
}
EDGE_ATTR_TEMPLATES = {
    'start_date': {'lived_in': "{subj}'s residence in {obj} began around {val}.", 'worked_at': "{subj} started working at {obj} around {val}.", 'member_of': "{subj}'s membership with {obj} began around {val}.", 'spouse_of': "{subj} and {obj} married around {val}.", 'partnered_with': "The partnership between {subj} and {obj} started around {val}.", 'default': "The '{rel}' connection between {subj} and {obj} likely began around {val}."},
    'end_date': {'lived_in': "{subj} stopped residing in {obj} around {val}.", 'worked_at': "{subj} left their position at {obj} around {val}.", 'member_of': "{subj}'s membership with {obj} ended around {val}.", 'spouse_of': "The marriage between {subj} and {obj} ended around {val}.", 'partnered_with': "The partnership between {subj} and {obj} concluded around {val}.", 'default': "The '{rel}' connection between {subj} and {obj} likely concluded around {val}."},
    'role': {'worked_at': "While at {obj}, {subj}'s role was {val}.", 'member_of': "As a member of {obj}, {subj} served as {val}.", 'participated_in': "During the event '{obj}', {subj}'s role was {val}.", 'default': "{subj}'s role regarding {obj} (relation: {rel}) was {val}."},
    'relationship_type': {'knows': "The nature of the connection between {subj} and {obj} was primarily {val}.", 'default': "The type of '{rel}' relationship between {subj} and {obj} was {val}."},
    'degree': "{subj} obtained a {val} degree from {obj}.", 'major': "{subj}'s major field of study at {obj} was {val}.", 'graduation_year': "{subj} graduated from {obj} around {val}.", 'thesis_topic': "At {obj}, {subj}'s thesis explored '{val}'.",
    'year': {'authored': "{subj} authored '{obj}' around {val}.", 'created': "{subj} created '{obj}' around {val}.", 'founded': "{subj} founded {obj} around {val}.", 'invested_in': "{subj} invested in {obj} around {val}.", 'default': "The '{rel}' interaction involving {subj} and {obj} occurred around {val}."},
    'amount': "{subj}'s investment in {obj} was approximately {val}.", 'impact': "Experiencing '{obj}' had a significant impact on {subj}, described as: {val}.", 'context': "The rivalry between {subj} and {obj} occurred within the {val} context.", 'industry': "The competition between {subj} and {obj} was notable in the {val} industry.", 'nature': "A consequence of '{subj}' leading to '{obj}' involved {val}.", 'reason': "Regarding {obj}, {subj} moved there, reportedly due to {val}.", 'project': "{subj} and {obj} collaborated on a project concerning '{val}'.", 'significance': "The significance of '{subj}' causing '{obj}' was rated as {val}.", 'medium': "The medium employed by {subj} for '{obj}' was {val}.",
    'description': None, 'default': None
}
def get_node_name(node_id, node_lookup, default_prefix="Entity"):
    node = node_lookup.get(node_id)
    if node:
        name = node.get('attributes', {}).get('name')
        if name and isinstance(name, str): return name.strip()
        node_type = node.get('type', default_prefix)
        node_type_str = str(node_type).lower() if node_type else default_prefix.lower()
        node_id_short = str(node_id)[:6] if node_id else 'unknown'
        if node_type == 'Person': return f"an unnamed individual (ID:{node_id_short})"
        if node_type == 'Organization': return f"an unnamed organization (ID:{node_id_short})"
        return f"the {node_type_str} (ID:{node_id_short})"
    node_id_short = str(node_id)[:6] if node_id else 'unknown'
    return f"an unknown entity ({node_id_short})"
    
def kg_to_sentences(kg_data, protagonist_id, max_distance=2): # max_distance is less relevant now
    sentences = []
    nodes = kg_data.get('nodes', [])
    edges = kg_data.get('edges', [])
    if not nodes: # No need to check protagonist_id here, just data presence
        return []

    node_lookup = {node.get('id'): node for node in nodes if node.get('id')}
    # Protagonist info might still be useful for filtering *which* facts to include later,
    # but for 1:1 mapping, we process all facts in the subgraph.
    # protagonist_node = node_lookup.get(protagonist_id)
    # protagonist_name = get_node_name(protagonist_id, node_lookup) if protagonist_node else "Protagonist"

    processed_facts = set() # Use a single set to track all processed facts (node attr, edge, edge attr)

    # --- Define which edge attributes to convert to sentences ---
    # --- Should ideally match 'edge_attributes_to_include' in extract_triples_from_subgraph ---
    edge_attributes_to_sentence = {
        'role', 'start_date', 'end_date', 'relationship_type', 'degree', 'major',
        'graduation_year', 'thesis_topic', 'year', 'amount', 'impact', 'context',
        'industry', 'nature', 'reason', 'project', 'significance', 'medium'
    }

    # 1. Process Node Attributes
    for node_id, node in node_lookup.items():
        node_name = get_node_name(node_id, node_lookup)
        attributes = node.get('attributes', {})
        node_type = node.get('type', 'default')

        for key, value in attributes.items():
            # Skip None/empty, name, and complex types
            if value not in [None, ""] and key != 'name' and not isinstance(value, (dict, list)):
                attr_fact_key = (node_id, key) # Fact: (node, attribute)
                if attr_fact_key not in processed_facts:
                    template = NODE_ATTR_TEMPLATES.get(key, NODE_ATTR_TEMPLATES.get('default'))
                    if template:
                        try:
                            formatted = template.format(subj=node_name, key=key, val=str(value))
                            sentences.append(formatted)
                            processed_facts.add(attr_fact_key)
                        except (KeyError, TypeError, ValueError) as e:
                            print(f"[WARN] Formatting error for node attr template (key={key}): {e}")
                            pass # Don't add fact if formatting fails

    # 2. Process Edges (Relation and Edge Attributes)
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        relation = edge.get('relation')
        edge_attrs = edge.get('attributes', {})

        # Ensure both ends are valid nodes in the current subgraph
        if source_id not in node_lookup or target_id not in node_lookup or not relation:
            continue

        source_node = node_lookup[source_id]
        target_node = node_lookup[target_id]
        source_name = get_node_name(source_id, node_lookup)
        target_name = get_node_name(target_id, node_lookup)

        # a. Process the core relation (Node-Rel-Node)
        relation_fact_key = (source_id, relation, target_id)
        if relation_fact_key not in processed_facts:
            s_name_fmt = f"'{source_name}'" if source_node.get('type') in ['Work', 'Event'] else source_name
            t_name_fmt = f"'{target_name}'" if target_node.get('type') in ['Work', 'Event'] else target_name
            template = RELATION_TEMPLATES.get(relation, RELATION_TEMPLATES.get('default'))
            if template:
                try:
                    formatted = template.format(subj=s_name_fmt, obj=t_name_fmt, rel=relation)
                    sentences.append(formatted)
                    processed_facts.add(relation_fact_key)
                except (KeyError, TypeError, ValueError) as e:
                    print(f"[WARN] Formatting error for relation template ({relation}): {e}")
                    pass # Don't add fact if formatting fails

        # b. Process Edge Attributes
        for attr_key, attr_value in edge_attrs.items():
            if attr_key in edge_attributes_to_sentence and attr_value not in [None, ""]:
                edge_attr_fact_key = (source_id, relation, attr_key) # Fact: (source_node, relation, attribute_key)
                if edge_attr_fact_key not in processed_facts:
                    template_or_dict = EDGE_ATTR_TEMPLATES.get(attr_key)
                    template = None
                    if isinstance(template_or_dict, dict):
                        # Get specific template for this relation or the default for the attribute
                        template = template_or_dict.get(relation, template_or_dict.get('default'))
                    elif isinstance(template_or_dict, str):
                        template = template_or_dict

                    if template:
                        try:
                             # Adjust subj/obj based on template needs if necessary (similar to previous logic)
                            subj_fmt_for_attr = source_name
                            obj_fmt_for_attr = target_name # Assume obj is target by default for attrs
                            if source_node.get('type') in ['Work', 'Event']: subj_fmt_for_attr = f"'{source_name}'"
                            if target_node.get('type') in ['Work', 'Event']: obj_fmt_for_attr = f"'{target_name}'"

                            # Handle relations where subject/object might be swapped in template context
                            # This part is tricky and depends heavily on template design.
                            # Example: 'participant' edge attr might need obj as subject in template.
                            # For simplicity now, assume template uses source as subj, target as obj.
                            # You might need more sophisticated template logic if this isn't sufficient.

                            formatted = template.format(subj=subj_fmt_for_attr, obj=obj_fmt_for_attr, rel=relation, key=attr_key, val=str(attr_value))
                            sentences.append(formatted)
                            processed_facts.add(edge_attr_fact_key)
                        except (KeyError, TypeError, ValueError) as e:
                            print(f"[WARN] Formatting error for edge attr template (key={attr_key}, rel={relation}): {e}")
                            pass # Don't add fact if formatting fails

    # --- REMOVED Interpretive Sentences Section ---
    # (No Capital Conversion, Background Influence, etc.)

    # 4. Final Formatting and Deduplication
    final_sentences_cleaned = []
    seen_sentences = set()
    for s in sentences: # Process sentences generated directly from facts
        s_clean = s.strip()
        if s_clean:
            if not s_clean.endswith(('.', '!', '?')):
                s_clean += '.'
            s_clean = s_clean[0].upper() + s_clean[1:]
            # Deduplicate exact sentence strings
            if s_clean not in seen_sentences:
                final_sentences_cleaned.append(s_clean)
                seen_sentences.add(s_clean)

    # Return in the order generated (more natural than sorting alphabetically)
    return final_sentences_cleaned

def extract_triples_from_subgraph(subgraph_data):
    """
    Extracts human-readable (Subject, Predicate, Object) triples
    from subgraph data for node attributes, relations, and edge attributes.
    """
    triples = []
    nodes = subgraph_data.get('nodes', [])
    edges = subgraph_data.get('edges', [])
    node_lookup = {node.get('id'): node for node in nodes if node.get('id')}

    # Helper for node attribute predicates (unchanged)
    def get_attribute_predicate(key):
        # ... (keep existing logic) ...
        if key == 'description': return "description"
        if key == 'birth_year': return "birth year"
        if key == 'death_year': return "death year"
        # ... etc ...
        return key.replace('_', ' ')

    # --- Process Node Attributes (Unchanged) ---
    processed_node_facts = set() # Track (node_id, key)
    for node in nodes:
        node_id = node.get('id')
        if not node_id: continue
        subj_name = get_node_name(node_id, node_lookup, default_prefix=node.get('type', 'Entity'))
        attributes = node.get('attributes', {})
        for key, value in attributes.items():
            fact_key = (node_id, key)
            if value not in [None, ""] and key != 'name' and fact_key not in processed_node_facts:
                 predicate = get_attribute_predicate(key)
                 value_str = str(value).replace('\t', ' ').replace('\n', ' ')
                 triples.append((subj_name, predicate, value_str))
                 processed_node_facts.add(fact_key)

    # --- Process Edges (Relations and Edge Attributes) ---
    processed_edge_facts = set() # Track (source_id, relation, target_id) and (source_id, relation, attr_key)
    edge_attributes_to_include = { # Define which edge attributes generate triples/sentences
        'role', 'start_date', 'end_date', 'relationship_type', 'degree', 'major',
        'graduation_year', 'thesis_topic', 'year', 'amount', 'impact', 'context',
        'industry', 'nature', 'reason', 'project', 'significance', 'medium'
        # Add or remove keys as needed
    }

    for edge in edges:
        source_id = edge.get('source')
        relation = edge.get('relation')
        target_id = edge.get('target')
        edge_attrs = edge.get('attributes', {})

        # Ensure both ends are in the subgraph lookup
        if source_id in node_lookup and relation and target_id in node_lookup:
            subj_name = get_node_name(source_id, node_lookup)
            obj_name = get_node_name(target_id, node_lookup)
            predicate = relation.replace('_', ' ')

            # 1. Add the core relation triple if not already processed
            relation_fact_key = (source_id, relation, target_id)
            if relation_fact_key not in processed_edge_facts:
                triples.append((subj_name, predicate, obj_name))
                processed_edge_facts.add(relation_fact_key)

            # 2. Add triples for edge attributes
            for attr_key, attr_value in edge_attrs.items():
                # Only include specified attributes and non-empty values
                if attr_key in edge_attributes_to_include and attr_value not in [None, ""]:
                    edge_attr_fact_key = (source_id, relation, attr_key) # Key to prevent duplicates for the same edge attr
                    if edge_attr_fact_key not in processed_edge_facts:
                        # Create a combined predicate: "relation attribute_key"
                        edge_attr_predicate = f"{predicate} {attr_key.replace('_', ' ')}"
                        value_str = str(attr_value).replace('\t', ' ').replace('\n', ' ')
                        # The subject is the source of the original edge
                        triples.append((subj_name, edge_attr_predicate, value_str))
                        processed_edge_facts.add(edge_attr_fact_key)

    # Remove exact duplicate triples *after* generation
    # Sorting helps ensure consistent output order if needed later, but set handles uniqueness
    unique_triples = sorted(list(set(triples)))
    return unique_triples

# --- Visualization Function ---
def visualize_kg(kg_data, protagonist_id, filename="knowledge_graph.png", layout_prog='sfdp', output_format='png'):
    if not HAS_NETWORKX or not HAS_PYGRAPHVIZ:
        print("[WARN] NetworkX/PyGraphviz not found. Skipping visualization.")
        return # Return early if libs missing

    nodes = kg_data.get('nodes', [])
    edges = kg_data.get('edges', [])
    node_lookup = {node.get('id'): node for node in nodes if node.get('id')}

    if not nodes or not node_lookup:
        print(f"[WARN] No nodes/lookup for visualization ({filename}).")
        return # Return early if no data

    G = nx.DiGraph()
    type_styles = {
        'Person': {'color': '#A7C7E7', 'shape': 'ellipse'}, 'Place': {'color': '#C1E1C1', 'shape': 'box'},
        'Organization': {'color': '#FADADD', 'shape': 'Mrecord'}, 'Work': {'color': '#FFFACD', 'shape': 'note'},
        'Event': {'color': '#FFDAB9', 'shape': 'diamond'}, 'default': {'color': '#E0E0E0', 'shape': 'ellipse'}
    }

    for node in nodes:
        node_id = node.get('id')
        if not node_id:
            continue
        attrs = node.get('attributes', {})
        node_type = node.get('type', 'Unknown')
        name = attrs.get('name', node_id[:8])
        name_str = str(name) if name is not None else node_id[:8]
        style = type_styles.get(node_type, type_styles['default'])
        display_name = (name_str[:25] + '...') if len(name_str) > 28 else name_str
        display_name = display_name.replace('"', '\\"').replace('\n', '\\n').replace(':', ';')
        node_attrs_for_viz = {
            'label': display_name, 'fillcolor': style.get('color', '#E0E0E0'),
            'shape': style.get('shape', 'ellipse'), 'style': 'filled', 'fontsize': 10
        }
        if node_id == protagonist_id:
            node_attrs_for_viz.update({'color': 'red', 'penwidth': 2.5, 'fontsize': 12, 'fontcolor': 'black', 'fillcolor': '#FFB6C1'})
        elif node_type == 'Event' and attrs.get('event_type','').startswith('Turning Point'):
            node_attrs_for_viz.update({'color': 'purple', 'penwidth': 1.5, 'fontcolor': 'black', 'fillcolor': '#E6E6FA'})
        G.add_node(node_id, **node_attrs_for_viz)

    edge_colors = {
        'knows': 'grey50', 'spouse_of': 'deeppink', 'child_of': 'blue', 'parent_of': 'blue',
        'rival_of': 'darkorange', 'competitor_of': 'darkred', 'influenced_by': 'green',
        'influenced': 'darkgreen', 'worked_at': 'black', 'member_of': 'black', 'default': 'grey70'
    }
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        relation = edge.get('relation', '')
        is_protagonist_edge = (source_id == protagonist_id or target_id == protagonist_id)
        rel_str = str(relation) if relation else ''
        if source_id in G and target_id in G:
            edge_color = edge_colors.get(rel_str, edge_colors['default'])
            arrowhead_style = 'none' if rel_str in ['knows', 'spouse_of', 'rival_of', 'competitor_of', 'partnered_with'] else 'normal'
            pen_width = 1.2 if is_protagonist_edge else 0.8
            edge_label = rel_str.replace('_', ' ')
            G.add_edge(
                source_id, target_id, label=edge_label, fontsize=8, fontcolor='dimgrey',
                color=edge_color, arrowhead=arrowhead_style, penwidth=pen_width,
                tooltip=f"{source_id} -> {target_id} ({rel_str})"
             )

    if len(G) == 0:
        print(f"[WARN] NetworkX graph empty after processing ({filename}). Skipping visualization.")
        return # Return early if graph is empty

    # print(f"[INFO] Creating visualization ({filename}) using Graphviz layout '{layout_prog}'...") # Moved to main loop
    try:
        A = nx.drawing.nx_agraph.to_agraph(G)
        A.graph_attr.update(
            rankdir='LR', splines='true', overlap='prism', nodesep=0.6, ranksep=1.2,
            dpi=150, concentrate=False, fontname='Helvetica', fontsize=10
        )
        A.node_attr.update(fontname='Helvetica', fontsize=10, shape='ellipse')
        A.edge_attr.update(fontname='Helvetica', fontsize=8, color='gray50', penwidth=0.8)
        A.layout(prog=layout_prog)
        A.draw(filename, format=output_format)
    except ImportError:
        print("[ERROR] PyGraphviz AGraph import failed.")
    except FileNotFoundError:
        print(f"[ERROR] Graphviz layout program '{layout_prog}' not found.")
    except Exception as e:
        print(f"[ERROR] Failed visualization ({filename}) using '{layout_prog}'. Error: {e}")
        if layout_prog != 'dot':
            print(f"[INFO] Retrying visualization ({filename}) with 'dot' layout...")
            try:
                # Need to re-create AGraph if layout failed? Maybe not.
                A.layout(prog='dot')
                A.draw(filename, format=output_format)
                # Print success only if retry works, handled in main loop now
            except Exception as e_dot:
                 print(f"[ERROR] Visualization ({filename}) failed even with 'dot' layout: {e_dot}")


# --- JSON Date Encoder ---
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, uuid.UUID):
            return str(obj)
        try:
            return super().default(obj)
        except TypeError:
             return str(obj) # Convert unknown types to string


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate multiple fictional KGs with sociological depth, optionally save files, and merge sentences.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--num-datasets", type=int, default=DEFAULT_NUM_DATASETS, help="Number of datasets to generate.")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Base directory for saving individual files within subdirs.")
    parser.add_argument("--name-prefix", type=str, default=None, help="Prefix for generated character names.")
    parser.add_argument("--archetype", type=str, default=None, choices=list(ARCHETYPES.keys()), help="Force a specific archetype (default: random).")
    parser.add_argument("--size", type=int, default=None, help="Target number of nodes per KG.")
    parser.add_argument("--max-distance", type=int, default=DEFAULT_MAX_DISTANCE, help="Max graph distance for NL sentence focus and subgraph saving.")
    parser.add_argument("--viz-prog", type=str, default=DEFAULT_VIZ_PROG, choices=['dot', 'neato', 'fdp', 'sfdp', 'twopi', 'circo'], help="Graphviz layout program.")
    parser.add_argument("--viz-format", type=str, default=DEFAULT_VIZ_FORMAT, choices=['png', 'svg', 'pdf', 'jpg'], help="Output format for visualization.")
    parser.add_argument("--no-kg", action='store_true', help="Do not save individual KG JSON files.")
    parser.add_argument("--no-sentences", action='store_true', help="Do not save individual sentences JSON files (still generated for merge).")
    parser.add_argument("--no-viz", action='store_true', help="Do not generate individual visualizations (for both full and subgraphs).")
    parser.add_argument("--no-merge", action='store_true', help="Do not perform the final sentence merging step.")
    parser.add_argument("--no-save-subgraph", dest='save_subgraph', action='store_false', help="Do NOT save the subgraph subset used for sentence generation (default: save subgraph).")
    parser.add_argument("--no-triples", action='store_true', help="Do not save individual subgraph triples TSV files.")
    parser.set_defaults(save_subgraph=True)
    args = parser.parse_args()

    base_output_dir = args.output_dir
    kg_subdir = os.path.join(base_output_dir, 'kg')
    subgraph_subdir = os.path.join(base_output_dir, 'subkg')
    sentences_subdir = os.path.join(base_output_dir, 'sentences')
    viz_subdir = os.path.join(base_output_dir, 'viz')
    triples_subdir = os.path.join(base_output_dir, 'triples')
    subviz_subdir = os.path.join(base_output_dir, 'subviz')

    try:
        if not args.no_kg: os.makedirs(kg_subdir, exist_ok=True)
        if args.save_subgraph: os.makedirs(subgraph_subdir, exist_ok=True)
        if not args.no_sentences: os.makedirs(sentences_subdir, exist_ok=True)
        if not args.no_viz:
            os.makedirs(viz_subdir, exist_ok=True)
            os.makedirs(subviz_subdir, exist_ok=True)
        if not args.no_triples: os.makedirs(triples_subdir, exist_ok=True)
        os.makedirs(base_output_dir, exist_ok=True)
    except OSError as e:
        print(f"[ERROR] Could not create output directories in '{base_output_dir}'. Please check permissions. Error: {e}")
        sys.exit(1)

    all_character_sentence_data = []
    start_time_total = time.time()

    for i in range(1, args.num_datasets + 1):
        run_start_time = time.time()
        print(f"\n--- Generating Dataset {i}/{args.num_datasets} ---")

        target_size_for_this_run = args.size
        if target_size_for_this_run is None:
            target_size_for_this_run = random.choice(DEFAULT_TARGET_NODE_COUNT_OPTIONS)
        print(f"[INFO] Target Node Count for this dataset: {target_size_for_this_run}")

        name_part = f"{fake.first_name()} {fake.last_name()}"
        title_part = args.name_prefix if args.name_prefix else random.choice(['Professor', 'Doctor', 'Madame', 'Director', 'Chancellor', 'Reverend', 'General', 'Ambassador', 'Agent', 'Captain', 'Comrade', 'Citizen', 'Mx'])
        fictional_character_name = f"{title_part} {name_part}"

        char_name_slug = f"{i:05d}_{title_part.lower()}_{name_part.lower().replace(' ', '_')}"
        char_name_slug = re.sub(r'[^\w\-]+', '_', char_name_slug)
        char_name_slug = re.sub(r'_+', '_', char_name_slug).strip('_')
        if not char_name_slug:
            char_name_slug = f"{i:05d}_character_{uuid.uuid4().hex[:4]}"

        kg_output_filename = os.path.join(kg_subdir, f"{char_name_slug}_kg.json")
        subgraph_output_filename = os.path.join(subgraph_subdir, f"{char_name_slug}_subgraph.json")
        viz_output_filename = os.path.join(viz_subdir, f"{char_name_slug}_graph.{args.viz_format}")
        subviz_output_filename = os.path.join(subviz_subdir, f"{char_name_slug}_subgraph.{args.viz_format}")
        sentences_output_filename = os.path.join(sentences_subdir, f"{char_name_slug}_sentences.json")
        triples_output_filename = os.path.join(triples_subdir, f"{char_name_slug}_triples.tsv")

        print(f"[INFO] Character Name: {fictional_character_name}")
        print(f"[INFO] Filename Slug: {char_name_slug}")

        kg_data = {}
        protagonist_id = None
        run_error = False
        try:
            kg_data = generate_fictional_kg_rich(
                fictional_character_name,
                args.archetype,
                target_size_for_this_run 
            )
            actual_nodes = len(kg_data.get('nodes', []))
            actual_edges = len(kg_data.get('edges', []))
            print(f"[INFO] KG Generation Complete. Actual Nodes: {actual_nodes}, Edges: {actual_edges}")
            protagonist_node = next((n for n in kg_data.get('nodes', []) if n.get('type') == 'Person' and n.get('attributes', {}).get('name') == fictional_character_name), None)
            if protagonist_node:
                protagonist_id = protagonist_node.get('id')
            else:
                print(f"[ERROR] CRITICAL: Protagonist '{fictional_character_name}' not found in generated nodes for dataset {i}.")
                run_error = True
            if actual_nodes <= 1 and args.size > 1 and not run_error:
                print(f"[WARN] Generated graph for dataset {i} has only {actual_nodes} node(s). Expansion may have failed.")
        except Exception as e:
             print(f"[ERROR] CRITICAL ERROR during KG generation for dataset {i}: {e}")
             traceback.print_exc()
             run_error = True

        # 1. Save Full KG (Optional)
        if not args.no_kg and kg_data and not run_error:
            print(f"[INFO] Saving full KG to: {kg_output_filename}")
            try:
                with open(kg_output_filename, 'w', encoding='utf-8') as f:
                    json.dump(kg_data, f, ensure_ascii=False, indent=2, cls=DateEncoder)
            except Exception as e:
                print(f"[ERROR] Error saving KG file {kg_output_filename}: {e}")

        # Extract Subgraph Data
        subgraph_data = None
        relevant_node_ids = set()
        if protagonist_id and kg_data and not run_error:
            try:
                node_lookup_full = {n.get('id'): n for n in kg_data.get('nodes', []) if n.get('id')}
                if not node_lookup_full:
                    raise ValueError("Full node lookup is empty.")
                edges_full = kg_data.get('edges', [])
                distances = get_node_distances(protagonist_id, edges_full, node_lookup_full)
                relevant_node_ids = {protagonist_id}
                relevant_node_ids.update(node_id for node_id, dist in distances.items() if dist <= args.max_distance)
                if len(relevant_node_ids) <= 1 and len(node_lookup_full) > 1:
                    print(f"[WARN] Only protagonist node found within max_distance={args.max_distance}. Subgraph will be minimal.")
                subgraph_nodes = [node for node in kg_data.get('nodes', []) if node.get('id') in relevant_node_ids]
                subgraph_edges = [edge for edge in kg_data.get('edges', []) if edge.get('source') in relevant_node_ids and edge.get('target') in relevant_node_ids]
                subgraph_data = {'nodes': subgraph_nodes, 'edges': subgraph_edges}
                print(f"[INFO] Extracted subgraph with {len(subgraph_nodes)} nodes and {len(subgraph_edges)} edges (max_distance={args.max_distance}).")
            except Exception as e:
                print(f"[ERROR] Error during subgraph extraction for dataset {i}: {e}")
                traceback.print_exc()
                run_error = True
                subgraph_data = None

        # 2. Save Subgraph JSON (Optional, Default=True)
        if args.save_subgraph and subgraph_data is not None and not run_error:
            print(f"[INFO] Saving sentence-related subgraph KG to: {subgraph_output_filename}")
            try:
                with open(subgraph_output_filename, 'w', encoding='utf-8') as f:
                    json.dump(subgraph_data, f, ensure_ascii=False, indent=2, cls=DateEncoder)
            except Exception as e:
                print(f"[ERROR] Error saving subgraph file {subgraph_output_filename}: {e}")
        elif not args.save_subgraph:
            print("[INFO] Skipping subgraph KG saving as per --no-save-subgraph flag.")

        # Extract Triples from Subgraph
        extracted_triples = []
        if subgraph_data is not None and not run_error:
             try:
                 extracted_triples = extract_triples_from_subgraph(subgraph_data)
                 print(f"[INFO] Extracted {len(extracted_triples)} triples from subgraph.")
             except Exception as e:
                 print(f"[ERROR] Error extracting triples for dataset {i}: {e}")
                 traceback.print_exc()
                 run_error = True # Mark error if triple extraction fails

        # Save Triples (Optional)
        if not args.no_triples and extracted_triples and not run_error:
            print(f"[INFO] Saving subgraph triples to: {triples_output_filename}")
            try:
                with open(triples_output_filename, 'w', encoding='utf-8') as f_tsv:
                    f_tsv.write("Subject\tPredicate\tObject\n")
                    for subj, pred, obj in extracted_triples:
                        # Write tab-separated values
                        f_tsv.write(f"{subj}\t{pred}\t{obj}\n")
            except Exception as e:
                print(f"[ERROR] Error saving triples file {triples_output_filename}: {e}")
        elif not args.no_triples and not extracted_triples and not run_error:
             print("[WARN] No triples extracted from subgraph, skipping TSV save.")

        # 5. Generate & Collect/Save Sentences JSON (Optional Saving)
        nl_sentences = []
        if subgraph_data is not None and protagonist_id and not run_error:
            # Check if protagonist is actually in the subgraph before proceeding
            if any(n['id'] == protagonist_id for n in subgraph_data.get('nodes',[])):
                print(f"[INFO] Starting NL conversion using subgraph data...")
                try:
                    nl_sentences = kg_to_sentences(subgraph_data, protagonist_id, args.max_distance)
                    num_sentences = len(nl_sentences)
                    print(f"[INFO] NL Conversion Complete. Generated {num_sentences} sentences.")
                    current_char_sentence_data = {
                        "character_slug": char_name_slug,
                        "character_name": fictional_character_name,
                        "sentences": nl_sentences
                    }
                    if not args.no_merge:
                        all_character_sentence_data.append(current_char_sentence_data)

                    if not args.no_sentences:
                        if num_sentences > 0:
                            print(f"[INFO] Saving individual sentences to: {sentences_output_filename}")
                            try:
                                with open(sentences_output_filename, 'w', encoding='utf-8') as f:
                                    json.dump(current_char_sentence_data, f, ensure_ascii=False, indent=2)
                            except Exception as e:
                                print(f"[ERROR] Error saving sentences file {sentences_output_filename}: {e}")
                        else:
                            print("[WARN] No sentences generated, skipping individual sentences save.")
                except Exception as e:
                     print(f"[ERROR] Error during NL conversion for dataset {i}: {e}")
                     traceback.print_exc()
                     run_error = True
            else:
                 print(f"[WARN] Protagonist node missing from subgraph data. Skipping NL conversion.")
                 run_error = True # Treat as error if subgraph doesn't contain protagonist

        # Visualizations (Optional)
        if not args.no_viz:
            if HAS_NETWORKX and HAS_PYGRAPHVIZ:
                # 3. Visualize Full Graph
                if kg_data and protagonist_id and not run_error:
                    print(f"[INFO] Attempting full graph visualization...")
                    try:
                        visualize_kg(
                            kg_data, protagonist_id, filename=viz_output_filename,
                            layout_prog=args.viz_prog, output_format=args.viz_format
                        )
                        print(f"[INFO] Full graph visualization saved to: {viz_output_filename}")
                    except Exception as e:
                        print(f"[ERROR] Error during full graph visualization: {e}")
                elif not kg_data or not protagonist_id:
                     print("[WARN] Skipping full graph visualization due to missing data or protagonist ID.")

                # 4. Visualize Subgraph
                if subgraph_data is not None and protagonist_id and not run_error:
                     if any(n['id'] == protagonist_id for n in subgraph_data.get('nodes',[])):
                         print(f"[INFO] Attempting subgraph visualization...")
                         try:
                             visualize_kg(
                                 subgraph_data, protagonist_id, filename=subviz_output_filename,
                                 layout_prog=args.viz_prog, output_format=args.viz_format
                             )
                             print(f"[INFO] Subgraph visualization saved to: {subviz_output_filename}")
                         except Exception as e:
                             print(f"[ERROR] Error during subgraph visualization: {e}")
                     else:
                          print("[WARN] Skipping subgraph visualization because protagonist is missing from subgraph data.")
                elif subgraph_data is None and not run_error: # Only warn if no *other* error caused subgraph_data to be None
                      print("[WARN] Skipping subgraph visualization because subgraph data is missing.")

            else: # Missing libs
                 if i == 1: # Show warning only once per run
                    print("[WARN] Visualization skipped because required libraries (NetworkX, PyGraphviz) or Graphviz installation are missing.")

        run_end_time = time.time()
        print(f"--- Dataset {i} completed in {run_end_time - run_start_time:.2f} seconds. Status: {'OK' if not run_error else 'ERRORS'} ---")

    end_time_total = time.time()
    print(f"\n--- Script finished generating {args.num_datasets} datasets in {end_time_total - start_time_total:.2f} seconds ---")