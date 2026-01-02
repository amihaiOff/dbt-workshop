---
description: Complete guide for modifying workshop exercises with checklist, testing workflow, and common pitfalls
---

# Guide: How to Modify Workshop Exercises

This guide provides a comprehensive checklist for modifying exercises in the dbt workshop. Following this process ensures that changes are complete, testable, and students will have a smooth experience.

## 🎯 Overview

When modifying an exercise, you're changing a multi-part system:
1. **Exercise instructions** (HTML file) - what students read
2. **Solution code** (hidden in HTML) - what students reveal to check their work
3. **Actual implementation files** - models, configs, snapshots that make the solution work
4. **Reset script** - ensures files are properly set up when students reset to a session

All parts must be synchronized and tested together.

## ✅ Step-by-Step Checklist

### 1. Update the Exercise HTML File
- [ ] Modify exercise instructions in `exercises/sessionN_hands_on.html`
- [ ] Update the solution section (inside `<div id="solutionN" class="solution">`)
- [ ] Ensure solution shows the CORRECT format (e.g., YAML vs SQL for snapshots)
- [ ] Verify any code examples match the actual implementation
- [ ] Check that workflow steps are OUTSIDE the solution div (visible to students)
- [ ] Add any necessary warnings, info boxes, or documentation links
- [ ] Use collapsible sections for optional/advanced content

### 2. Create/Update Implementation Files
- [ ] Create or modify the actual files referenced in the solution (models, configs, etc.)
- [ ] Test each file individually with dbt commands
- [ ] Verify file locations match what's shown in the exercise
- [ ] Check for syntax errors, especially in:
  - Jinja syntax (no trailing commas in `{{ ... }}`)
  - YAML indentation and structure
  - SQL syntax and dbt functions
- [ ] Add log statements where helpful for student debugging

### 3. Update the reset_to_session.sh Script
**CRITICAL:** The reset script has multiple sections. Update ALL relevant ones:
- [ ] **Session 2 section**: If modifying session 2 exercises
- [ ] **Session 3 section**: If modifying session 3 exercises
- [ ] **End section** (`--end` flag): Should have ALL solutions from all sessions

For each section:
- [ ] Add/update file creation code (using heredoc for multi-line files)
- [ ] Match the EXACT content from your implementation files
- [ ] Verify indentation is preserved in heredocs
- [ ] If files should exist but be empty, create placeholder comments
- [ ] Update any dbt commands that need to run (dbt run, dbt snapshot, etc.)
- [ ] Update database verification queries if needed

### 4. Test the Complete Workflow

#### Reset and Build Test
```bash
# Test resetting to the session you modified
./reset_to_session.sh N

# Verify files were created
ls -la models/staging/
ls -la models/intermediate/
ls -la snapshots/

# Check file contents match expectations
cat path/to/your/file
```

#### dbt Commands Test
```bash
# Test building models
docker exec dbt-workshop dbt run --select your_model

# Test snapshots
docker exec dbt-workshop dbt snapshot

# Test with variables if applicable
docker exec dbt-workshop dbt run --vars '{"var_name": "value"}'
```

#### Database Verification
```bash
# Connect to database and verify data
docker exec dbt-workshop-postgres psql -U dbt_user -d dbt_workshop -c "
  SELECT * FROM schema.your_table LIMIT 10;
"

# Verify specific examples mentioned in the exercise
docker exec dbt-workshop-postgres psql -U dbt_user -d dbt_workshop -c "
  SELECT * FROM schema.your_table WHERE id = 'example_id';
"
```

#### End-to-End Student Workflow Test
- [ ] Reset to the session
- [ ] Follow the exercise instructions EXACTLY as a student would
- [ ] Reveal the solution
- [ ] Implement the solution (or verify it's already there)
- [ ] Run the dbt commands shown in the exercise
- [ ] Verify the expected output matches what the exercise says

### 5. Common Pitfalls to Avoid

#### ❌ YAML Configuration Issues
- Don't add redundant schema configs that cause double prefixing
- Example: In snapshots, if using `ref()`, don't also specify `schema` in config
- Always verify YAML syntax against dbt documentation

#### ❌ Solution Format Mismatch
- Exercise solution must match the ACTUAL format used (SQL vs YAML, etc.)
- Don't show SQL format if implementation uses YAML or vice versa

#### ❌ Incomplete Reset Script Updates
- Forgetting to update ALL session sections (session 2, 3, and end)
- Not matching exact content between implementation and reset script
- Missing dbt commands needed to populate data

#### ❌ Workflow Steps Hidden in Solution
- Instructions on HOW to do the challenge should be visible
- Only hide the actual CODE/configuration in the solution div
- Students need to see the steps before clicking "Show Solution"

#### ❌ Untested Examples
- Don't add specific examples (like a seller ID) without verifying they exist
- Always query the database to confirm example data is present

#### ❌ Variable Hierarchy Confusion
- When teaching variables, show progression: model default → dbt_project.yml → --vars flag
- Demonstrate each level with actual commands students can run

#### ❌ References to Compiled Code
- If mentioning `target/compiled` for verification, show exact path
- Keep it concise if it's a reminder (students saw it before)

### 6. Documentation and References

When adding external references:
- [ ] Link to official dbt docs for features being taught
- [ ] Mention if topic was covered in presentation
- [ ] Use info boxes for important conceptual explanations
- [ ] Add warning boxes for common mistakes

### 7. Example Workflow (Based on Snapshot YAML Conversion)

**What was changed:** Convert snapshots from SQL to YAML format

**Files modified:**
1. ✅ `exercises/session2_hands_on.html`
   - Updated solution to show YAML format
   - Removed schema config explanation
   - Added link to dbt snapshot docs

2. ✅ `snapshots/snapshots.yml` (created)
   - Used correct syntax: `relation: ref('model_name')`
   - Removed redundant `schema` config

3. ✅ `snapshots/snap_seller_tier.sql` (deleted)
   - Removed old SQL format file

4. ✅ `reset_to_session.sh`
   - Updated all 3 sections (session 2, 3, end)
   - Changed from SQL file creation to YAML
   - Kept dbt snapshot commands

**Testing performed:**
- ✅ Ran `./reset_to_session.sh 2`
- ✅ Verified snapshot creates as `olist_data.snap_seller_tier` (not double schema)
- ✅ Checked historical data with example seller ID
- ✅ Confirmed 4 snapshot iterations run successfully

## 🚀 Quick Reference Commands

```bash
# Test reset for specific session
./reset_to_session.sh N

# Test reset with all solutions
./reset_to_session.sh --end

# Run specific model
docker exec dbt-workshop dbt run --select model_name

# Run snapshot
docker exec dbt-workshop dbt snapshot

# Check database tables
docker exec dbt-workshop-postgres psql -U dbt_user -d dbt_workshop -c "\dt olist_data.*"

# Query specific table
docker exec dbt-workshop-postgres psql -U dbt_user -d dbt_workshop -c "SELECT * FROM schema.table LIMIT 10;"

# Check compiled SQL
docker exec dbt-workshop ls -la target/compiled/dbt_workshop/models/
docker exec dbt-workshop cat target/compiled/dbt_workshop/models/path/to/model.sql
```

## 📋 Final Checklist Before Committing

- [ ] All exercise HTML files updated with correct instructions and solutions
- [ ] All implementation files created/updated and tested independently
- [ ] reset_to_session.sh updated in ALL relevant sections
- [ ] Tested reset script runs without errors
- [ ] Verified dbt commands work as expected
- [ ] Checked database contains expected data
- [ ] Tested specific examples mentioned in exercise
- [ ] Followed complete student workflow successfully
- [ ] Git commit message describes what was changed and why
- [ ] No leftover test files or commented code

## 💡 Pro Tips

1. **Test incrementally**: Don't change everything at once. Modify one challenge, test it, commit it.

2. **Use git branches**: Create a branch for exercise modifications to keep work organized.

3. **Verify dbt docs**: When using dbt features, always check official documentation for correct syntax.

4. **Think like a student**: After making changes, try to follow the exercise as if you've never seen it before.

5. **Check both formats**: Some exercises show code in HTML and in actual files - keep them in sync.

6. **Database state matters**: Reset script should leave database in exact state needed for next exercises.

7. **Log statements help**: Add informative log messages in models to help students debug.

---

**Remember:** The goal is to make exercises clear, self-contained, and foolproof. If you're confused by an instruction, students will be too!
