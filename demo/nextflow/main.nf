nextflow.enable.dsl = 2

process WRITE_MESSAGE {
    output:
    path 'message.txt'

    script:
    """
    echo 'hello from Aruna TES' > message.txt
    """
}

process UPPERCASE {
    input:
    path message

    output:
    path 'uppercase.txt'

    script:
    """
    tr '[:lower:]' '[:upper:]' < ${message} > uppercase.txt
    """
}

process COUNT_WORDS {
    input:
    path message

    output:
    path 'summary.txt'

    script:
    """
    wc -w ${message} > summary.txt
    """
}

workflow {
    message = WRITE_MESSAGE()
    uppercase = UPPERCASE(message)
    COUNT_WORDS(uppercase).view { "Aruna result: ${it}" }
}
