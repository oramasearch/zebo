use clap::Parser;
use zebo::Zebo;

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct DocumentId(u64);

impl zebo::DocumentId for DocumentId {
    fn as_u64(&self) -> u64 {
        self.0
    }

    fn from_u64(id: u64) -> Self {
        DocumentId(id)
    }
}

#[derive(Parser)]
#[command(name = "zebo-inspect")]
#[command(about = "Inspect and validate Zebo page contents")]
#[command(version)]
struct Cli {
    /// Path to the zebo directory containing the index and page files
    zebo_path: String,

    /// Page ID to inspect
    #[arg(long, required = true)]
    page_id: u64,

    /// Skip expensive consistency checks (assertions and document retrieval validation)
    #[arg(long)]
    skip_content_checks: bool,

    /// Skip printing the actual document content
    #[arg(long)]
    skip_document_content: bool,

    /// Skip printing header information (version, counts, offsets)
    #[arg(long)]
    skip_header_info: bool,
}

// 1 GB
static PAGE_SIZE: u64 = 1024 * 1024 * 1024;

fn main() {
    const MAX_DOC_PER_PAGE: u32 = 1_000_000;

    let cli = Cli::parse();

    let zebo = Zebo::<MAX_DOC_PER_PAGE, PAGE_SIZE, DocumentId>::try_new(&cli.zebo_path)
        .expect("Failed to create Zebo instance");

    zebo.debug_content_with_options(
        cli.page_id,
        &mut std::io::stdout(),
        cli.skip_content_checks,
        cli.skip_document_content,
        cli.skip_header_info,
    )
    .expect("Failed to debug content");

    println!("Done");
}
