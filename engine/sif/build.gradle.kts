plugins {
    `java-library`
}

dependencies {
    // Core engine types (ontology mapping, schema)
    implementation(project(":core"))

    // Jackson for JSON parsing
    api("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testImplementation("org.assertj:assertj-core:3.26.3")
}
