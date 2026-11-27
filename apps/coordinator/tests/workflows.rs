//! Small workflows of each language the coordinator runs, as the node would send their files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_workflow_contract::preflight::{SourceFile, WorkflowLanguage, WorkflowSource};

/// One workflow: its files, its entry file and its language as the node states it.
pub struct Fixture {
    pub name: &'static str,
    pub files: &'static [(&'static str, &'static str)],
    pub entrypoint: &'static str,
    pub language: Option<WorkflowLanguage>,
}

impl Fixture {
    pub fn source(&self) -> WorkflowSource {
        source(self.files)
    }
}

pub fn source(files: &[(&str, &str)]) -> WorkflowSource {
    WorkflowSource {
        files: (files.iter())
            .map(|(path, text)| SourceFile {
                path: (*path).into(),
                content: text.as_bytes().to_vec(),
                executable: false,
            })
            .collect(),
    }
}

pub const NEXTFLOW: &str = r#"process GREET {
    container 'debian:stable-slim'

    input:
    val greeting

    output:
    path "${greeting}.txt"

    script:
    """
    echo '${greeting} world' > ${greeting}.txt
    """
}

workflow {
    GREET(channel.of('Hello', 'Hola'))
}
"#;

pub const SNAKEMAKE: &str = r#"workdir: workflow.basedir

GREETINGS = ["Hello", "Hola"]


rule all:
    input:
        "hello/all.txt"


rule greet:
    output:
        "hello/{greeting}.txt"
    container:
        "docker://debian:stable-slim"
    shell:
        "echo '{wildcards.greeting} world' > {output}"


rule collect:
    input:
        expand("hello/{greeting}.txt", greeting=GREETINGS)
    output:
        "hello/all.txt"
    container:
        "docker://debian:stable-slim"
    shell:
        "cat {input} > {output}"
"#;

pub const CWL: &str = r#"cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
hints:
  DockerRequirement:
    dockerPull: debian:stable-slim
inputs:
  message:
    type: string
    default: Hello
    inputBinding:
      position: 1
outputs:
  said:
    type: stdout
stdout: said.txt
"#;

pub const WDL: &str = r#"version 1.2

task say_hello {
  input {
    String greeting
  }

  command <<<
    echo "~{greeting}, world" > greeting.txt
  >>>

  output {
    File out = "greeting.txt"
  }

  requirements {
    container: "debian:stable-slim"
  }
}

workflow hello {
  input {
    String greeting = "Hello"
  }

  call say_hello { input: greeting = greeting }

  output {
    File greeting_file = say_hello.out
  }
}
"#;

pub const ARGO: &str = r#"apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: hello-
spec:
  entrypoint: hello
  templates:
  - name: hello
    container:
      image: debian:stable-slim
      command: [echo]
      args: ["hello world"]
"#;

pub const GALAXY: &str = r#"<?xml version="1.0"?>
<tool id="count_lines" name="Count lines" version="1.0" profile="22.05">
    <requirements>
        <container type="docker">debian:stable-slim</container>
    </requirements>
    <command detect_errors="exit_code"><![CDATA[
        wc -l < '$input' > '$summary'
    ]]></command>
    <inputs>
        <param name="input" type="data" format="txt" label="Text to count"/>
    </inputs>
    <outputs>
        <data name="summary" format="tabular" label="Line count"/>
    </outputs>
</tool>
"#;

pub const WIR: &str = r#"wir_version: "0.1"
sources: []
graphs:
  - name: hello
    inputs:
      - name: name
        type: string
        default: world
    outputs:
      - name: greeting
        type: {file: {}}
    ops:
      - path: greet
        kind:
          task:
            image: debian:stable-slim
            command: [echo, Hello, {input: name}]
            captures: [{output: greeting, from: stdout}]
        inputs: [{name: name, type: string}]
        outputs: [{name: greeting, type: {file: {}}}]
    edges:
      - {from: {port: name}, to: {op: greet, port: name}}
      - {from: {op: greet, port: greeting}, to: {port: greeting}}
entry: hello
"#;

/// An Argo container template, which uwe reads as one tool.
pub const REVERSE_TOOL: &str = r#"apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: reverse
spec:
  entrypoint: reverse
  templates:
    - name: reverse
      inputs:
        artifacts:
          - name: text
            path: /tmp/in/text.txt
      container:
        image: debian:stable-slim
        command: [sh, -c]
        args: ["tac /tmp/in/text.txt > /tmp/reversed.txt"]
      outputs:
        artifacts:
          - name: reversed
            path: /tmp/reversed.txt
            archive:
              none: {}
"#;

/// An Argo script template that writes a text in capital letters.
pub const SHOUT_TOOL: &str = r#"apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: shout
spec:
  entrypoint: shout
  templates:
    - name: shout
      inputs:
        artifacts:
          - name: text
            path: /tmp/in/text.txt
      container:
        image: debian:stable-slim
        command: [sh, -c]
        args: ["tr a-z A-Z < /tmp/in/text.txt > /tmp/shouted.txt"]
      outputs:
        artifacts:
          - name: shouted
            path: /tmp/shouted.txt
            archive:
              none: {}
"#;

/// A composed workflow of two steps with Aruna settings on both.
pub const COMPOSED: &str = r#"uwe_version: "1"
name: twice
description: Reverses a text and writes it in capital letters.
tools:
  - tools/reverse.yaml
  - tools/shout.yaml
inputs:
  text:
    type: file
    description: The text to reverse.
steps:
  first:
    tool: reverse
    in:
      text: text
    aruna:
      resources:
        cpus: 2
        memory_gib: 1.5
      execution:
        network: false
  second:
    tool: shout
    in:
      text: first/reversed
    aruna:
      placement:
        site: giessen
outputs:
  result:
    from: second/shouted
"#;

pub const FIXTURES: [Fixture; 8] = [
    Fixture {
        name: "nextflow",
        files: &[("main.nf", NEXTFLOW)],
        entrypoint: "main.nf",
        language: Some(WorkflowLanguage::Nextflow),
    },
    Fixture {
        name: "snakemake",
        files: &[("Snakefile", SNAKEMAKE)],
        entrypoint: "Snakefile",
        language: Some(WorkflowLanguage::Snakemake),
    },
    Fixture {
        name: "cwl",
        files: &[("hello.cwl", CWL)],
        entrypoint: "hello.cwl",
        language: Some(WorkflowLanguage::Cwl),
    },
    Fixture {
        name: "wdl",
        files: &[("hello.wdl", WDL)],
        entrypoint: "hello.wdl",
        language: Some(WorkflowLanguage::Wdl),
    },
    Fixture {
        name: "argo",
        files: &[("hello.yaml", ARGO)],
        entrypoint: "hello.yaml",
        language: Some(WorkflowLanguage::Argo),
    },
    Fixture {
        name: "galaxy",
        files: &[("count.xml", GALAXY)],
        entrypoint: "count.xml",
        language: Some(WorkflowLanguage::Galaxy),
    },
    Fixture {
        name: "wir",
        files: &[("hello.wir.yaml", WIR)],
        entrypoint: "hello.wir.yaml",
        language: None,
    },
    Fixture {
        name: "composed",
        files: &[
            ("twice.uwe.yaml", COMPOSED),
            ("tools/reverse.yaml", REVERSE_TOOL),
            ("tools/shout.yaml", SHOUT_TOOL),
        ],
        entrypoint: "twice.uwe.yaml",
        language: None,
    },
];
