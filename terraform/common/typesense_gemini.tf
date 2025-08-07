resource "google_project_service" "generative_language_api" {
  project = local.default_project
  service = "generativelanguage.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "apikeys_api" {
  project = local.default_project
  service = "apikeys.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_project_service" "secretmanager_api" {
  project = local.default_project
  service = "secretmanager.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

resource "google_apikeys_key" "typesense_gemini_api_key" {
  name         = "typesense-gemini-api-key"
  display_name = "typesense-gemini-api-key"

  restrictions {
    # Restrict to Generative Language API only
    api_targets {
      service = "generativelanguage.googleapis.com"
    }
  }
  depends_on = [
    google_project_service.generative_language_api,
    google_project_service.apikeys_api
  ]
}

output "typesense_gemini_api_key" {
  description = "The Gemini API key for Typesense"
  value       = google_apikeys_key.typesense_gemini_api_key.key_string
  sensitive   = true
}

output "typesense_gemini_api_key_name" {
  description = "The name of the Gemini API key"
  value       = google_apikeys_key.typesense_gemini_api_key.name
}
