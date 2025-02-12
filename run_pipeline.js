import { ApifyClient } from "apify-client";
import fs from "fs";
import { execSync } from "child_process";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config();
console.log("Starting pipeline script...");

// Initialize Apify client
const client = new ApifyClient({ token: process.env.APIFY_TOKEN });

// Initialize Supabase client
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

// 1️⃣ Run Apify Scraper
async function scrapeJobs() {
    console.log("✅ Running Apify scraper...");
    const inputJSON = {
        country: "IN",
        followApplyRedirects: true,
        maxItems: 15,
        parseCompanyDetails: true,
        position: "Data Scientist, Business Analyst, Data Engineer, Python Developer, Full Stack Developer, Machine Learning Engineer, Software Engineer, Backend Developer, Frontend Developer, AI Engineer, DevOps Engineer",
        saveOnlyUniqueItems: true
    };

    try {
        const actorRun = await client.actor("hMvNSpz3JnHgl5jkh").call(inputJSON);

        if (!actorRun.defaultDatasetId) {
            console.error("❌ No dataset ID found.");
            process.exit(1);
        }

        const datasetId = actorRun.defaultDatasetId;
        const { items } = await client.dataset(datasetId).listItems();

        if (items.length === 0) {
            console.log("⚠️ No jobs scraped.");
        } else {
            fs.writeFileSync("scraped_jobs.json", JSON.stringify(items, null, 2));
            console.log("✅ Scraped jobs saved.");
        }
    } catch (error) {
        console.error("❌ Scraping failed:", error);
        process.exit(1);
    }
}

// 2️⃣ Format Jobs (Runs Python Script)
function formatJobs() {
    console.log("✅ Formatting job data...");
    try {
        execSync("python format_jobs.py", { stdio: "inherit" });
        console.log("✅ Formatting complete.");
    } catch (error) {
        console.error("❌ Formatting failed:", error);
        process.exit(1);
    }
}

// 3️⃣ Upload Jobs to Supabase (With Duplicate Prevention)
async function uploadJobs() {
    console.log("✅ Uploading jobs to Supabase...");

    try {
        const jobs = JSON.parse(fs.readFileSync("formatted_jobs.json", "utf8"));

        for (const job of jobs) {
            const { job_url, ...jobData } = job; // Separate job_url for checking

            // 🔍 Check if the job already exists in Supabase
            const { data: existingJob, error: fetchError } = await supabase
                .from("jobs")
                .select("job_url")
                .eq("job_url", job_url)
                .single();

            if (fetchError && fetchError.code !== "PGRST116") {
                console.error("❌ Error checking existing job:", fetchError);
                continue;
            }

            if (existingJob) {
                console.log(`⚠️ Skipping duplicate job: ${job.title}`);
                continue;
            }

            // 📥 Insert new job
            const { error: insertError } = await supabase
                .from("jobs")
                .insert([{ job_url, ...jobData }]);

            if (insertError) {
                console.error("❌ Upload error:", insertError);
            } else {
                console.log(`✅ Job '${job.title}' uploaded.`);
            }
        }
    } catch (error) {
        console.error("❌ Uploading failed:", error);
        process.exit(1);
    }
}

// 4️⃣ Run Full Pipeline
async function runPipeline() {
    await scrapeJobs();
    formatJobs();
    await uploadJobs();
    console.log("🚀 Pipeline completed successfully!");
}

runPipeline();
