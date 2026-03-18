import re

with open('admin_panel.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# We'll process line by line, but need to handle multi-line labels? Simpler: replace pattern.
# We'll join lines, process, then split.
content = ''.join(lines)

# Pattern to find: <label>[^<]*</label> followed by whitespace and then an input/select/textarea with id
# We'll do a simpler approach: find all labels that do not have a 'for' attribute and are followed by a field with id.
# We'll use regex to replace.

# First, find labels without for attribute that are immediately before a field with id.
# We'll look for: <label[^>]*>[^<]*</label>\s*<(input|select|textarea)[^>]*id="([^"]*)"
# But note there might be other tags in between? In our HTML, it's usually directly followed by the field.

# Let's do a simple iterative replacement: we'll scan the content and replace when we see a label without for followed by a field.

# We'll use a while loop to find and replace.
def add_for_attribute(content):
    # Pattern for a label without for attribute
    label_pattern = r'<label([^>]*)>([^<]*)</label>'
    # We'll iterate over matches and check if the next non-whitespace tag is an input/select/textarea with id.
    # Instead, we'll do a simpler: replace all labels that are followed by a field with id in the same fg div.
    # Given the structure, we can assume that within a <div class="fg">, the label is followed by the field.
    # So we can target <div class="fg">.*?<label([^>]*)>([^<]*)</label>\s*<(input|select|textarea)[^>]*id="([^"]*)"
    # But this is complex.

    # Let's do a simpler: we'll just add for attribute to labels that are followed by a field with id in the same line or next line.
    # We'll process line by line.

    new_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Check if this line contains a label without for attribute
        if '<label' in line and 'for=' not in line:
            # Look ahead for the next line that contains an input/select/textarea with id
            j = i + 1
            while j < len(lines) and ('<' not in lines[j] or 'id=' not in lines[j]):
                j += 1
            if j < len(lines):
                # Extract the id from the line j
                match = re.search(r'id="([^"]*)"', lines[j])
                if match:
                    field_id = match.group(1)
                    # Add for attribute to the label in line i
                    # Replace <label ...> with <label ... for="field_id">
                    # We need to insert for attribute before the closing >
                    new_line = re.sub(r'(<label[^>]*)>', r'\1 for="' + field_id + '">', line)
                    new_lines.append(new_line)
                    i += 1
                    continue
        new_lines.append(line)
        i += 1
    return ''.join(new_lines)

new_content = add_for_attribute(content)

with open('admin_panel.html', 'w', encoding='utf-8') as f:
    f.write(new_content)

print('Fixed labels in admin_panel.html')