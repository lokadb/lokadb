<?php

namespace App\Console\Commands;

use App\Models\Comment;
use App\Models\Document;
use App\Models\Organization;
use App\Models\Project;
use Illuminate\Console\Command;

class DemoSeed extends Command
{
    protected $signature = 'demo:seed';

    protected $description = 'Seed two organizations with documents';

    public function handle(): int
    {
        foreach (['A', 'B'] as $l) {
            $org = Organization::create(['name' => "Org $l"]);
            $project = Project::create(['organization_id' => $org->id, 'name' => "$l-Project"]);
            for ($i = 1; $i <= 3; $i++) {
                $doc = Document::create([
                    'organization_id' => $org->id, 'project_id' => $project->id,
                    'slug' => strtolower($l)."-$i", 'title' => "$l doc $i", 'body' => "secret $l $i",
                ]);
                $doc->versions()->create(['content' => 'v1']);
                $doc->versions()->create(['content' => 'v2']);
                Comment::create(['document_id' => $doc->id, 'author' => 'alice', 'body' => 'looks good']);
            }
        }
        $this->info('seeded');

        return self::SUCCESS;
    }
}
