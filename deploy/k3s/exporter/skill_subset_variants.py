import yaml

source_tasks_dir = "/path/to/harbor_tasks"
variants_dir = "./variants/"

def generated_prompt(subsets: set()) -> str:
    # Append this to all instruction.md
    instruction_addenum = f"""You should use the following {len(subsets)} skills to help you accomplish the feature implementation: {subsets}"""

    return instruction_addenum

def get_subsets(skills: list, min_enabled = 1, always_on_skills = {}) -> list[set]:
    subsets = []
    # 2^n possible unique sets of skills
    for i in range(1, 1 << len(skills)):
        subset = {skills[j] for j in range(len(skills)) if (i & (1 << j))} | always_on_skills
        subsets.append(subset)
    
    subsets = [s for s in subsets if len(s) >= min_enabled]

    return subsets

if __name__ == "__main__":
    with open('skill_map.yaml', 'r') as file:
        skill_map = yaml.safe_load(file)
        loaded_skills = skill_map["skills"]
        always_on_skills = skill_map["always-on"]
        subsets = get_subsets(loaded_skills, 4, set(always_on_skills))
        print(len(subsets))
        for s in subsets:
            generated_prompt(s)
            