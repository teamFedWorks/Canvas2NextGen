import shutil, os

src = r'storage/uploads/BS_Computer_Science/01_-_PHI-1114_Logic_and_Argumentation'
dst = r'storage/uploads/WBU/01_-_PHI-1114_Logic_and_Argumentation'

print('Source exists?', os.path.exists(src))
print('Dest exists?', os.path.exists(dst))

if os.path.exists(src) and not os.path.exists(dst):
    shutil.move(src, dst)
    print('Moved successfully')
    print('New location:', dst)
    print('Dest exists now?', os.path.exists(dst))
else:
    print('Cannot move - check paths')
