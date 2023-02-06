from setuptools import find_packages, setup

entry_point = (
    "diver-fi = diver_fi.__main__:main"
)


# instala as dependências
with open("requirements.txt", encoding="utf-8") as f:
    # Garantir que não haja modificações originadas de um pip.conf
    # quando rodarmos o kedro build-reqs
    requires = []
    for line in f:
        req = line.split("#", 1)[0].strip()
        if req and not req.startswith("--"):
            requires.append(req)

setup(
    name="diver_fi",
    version="0.1",
    packages=find_packages(exclude=["tests"]),
    entry_points={"console_scripts": [entry_point]},
    install_requires=requires,
    extras_require={
        "docs": [
            "docutils<0.18.0",
            "sphinx~=3.4.3",
            "sphinx_rtd_theme==0.5.1",
            "nbsphinx==0.8.1",
            "nbstripout~=0.4",
            "myst-parser~=0.17.2",
            "sphinx-autodoc-typehints==1.11.1",
            "sphinx_copybutton==0.3.1",
            "ipykernel>=5.3, <7.0",
            "Jinja2<3.1.0",
        ]
    },
)