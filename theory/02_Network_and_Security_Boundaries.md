# Network and Security Boundaries in Azure

Data pipelines must adapt their networking topology depending on where the source sits and how strict the security boundary is.

## 1. Public Internet (Secured)
* **When to use:** Ingesting from public REST APIs, third-party SaaS, or public cloud endpoints.
* **Mechanics:** Outbound requests travel over encrypted TLS 1.3. Source systems whitelist your Data Factory's regional IP range.

## 2. Private Network / Hybrid (Self-Hosted IR)
* **When to use:** Enterprise on-premises databases (SQL Server/Oracle), corporate file shares, or strictly regulated environments where internal data cannot traverse public IP routes.
* **Mechanics:** You utilize an Azure ExpressRoute or Site-to-Site (S2S) VPN. You deploy an ADF **Self-Hosted Integration Runtime (SHIR)** on an on-premises VM behind corporate firewalls. The SHIR initiates outbound connections to Azure over port 443.

## 3. Managed Virtual Network (VNet)
* **When to use:** Source and target both sit within the cloud, but compliance mandates zero traversal of the public internet.
* **Mechanics:** Connectivity to ADLS Gen2 or Azure SQL is established via **Managed Private Endpoints**.

## 4. Air-Gapped / Offline Physical Migration
* **When to use:** Bandwidth limitations prevent timely network transfer (e.g., migrating 2 Petabytes over a 100 Mbps uplink would take over a year).
* **Mechanics:** Request an **Azure Data Box Heavy**. Data is copied onto the device locally with AES-256 hardware encryption, then shipped physically back to an Azure datacenter.
