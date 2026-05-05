terraform {
  required_providers {
    motherduck = {
      source  = "matsonj/motherduck"
      version = "~> 0.1.0"
    }
  }
}

variable "motherduck_token" {
  description = "MotherDuck API token"
  type        = string
  sensitive   = true
}

provider "motherduck" {
  token = var.motherduck_token
}

# Create a new MotherDuck database
resource "motherduck_database" "my_db" {
  name = "noaa_database"
}

# Share the database with another user
resource "motherduck_share" "team_share" {
  database_name = motherduck_database.my_db.name
  share_name    = "noaa_team_share"
}
