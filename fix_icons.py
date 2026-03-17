import re

with open('admin_panel.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix patterns like 'fas fa-xxx"></i>' -> '<i class="fas fa-xxx"></i>'
content = re.sub(r'fas fa-([a-z0-9-]+)"></i>', r'<i class="fas fa-\1"></i>', content)

with open('admin_panel.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixed admin_panel.html')
