<?php

namespace App\Console\Commands;

use App\Models\Comment;
use App\Models\Document;
use App\Models\DocumentVersion;
use App\Models\Organization;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

// Ordinary Eloquent code: it knows nothing about where the rows physically live.
class DemoScenario extends Command
{
    protected $signature = 'demo:scenario {phase}';

    protected $description = 'Run the demo scenario (r1, r2, offline)';

    private int $fails = 0;

    public function handle(): int
    {
        $a = Organization::where('name', 'Org A')->firstOrFail();
        $b = Organization::where('name', 'Org B')->firstOrFail();
        match ($this->argument('phase')) {
            'r1' => $this->r1($a, $b),
            'r2' => $this->r2($a, $b),
            'offline' => $this->offline($a, $b),
        };

        return $this->fails === 0 ? self::SUCCESS : self::FAILURE;
    }

    private function r1(Organization $a, Organization $b): void
    {
        foreach ([[$a, 'A'], [$b, 'B']] as [$org, $l]) {
            $this->check("$l: list with eager-loaded project and versions", function () use ($org, $l) {
                $docs = Document::with(['project', 'versions'])->where('organization_id', $org->id)->orderBy('id')->get();

                return $docs->count() === 3
                    && $docs->every(fn ($d) => $d->project->name === "$l-Project" && $d->versions->count() === 2);
            });
            $this->check("$l: find by id (no tenant in the query)", function () use ($org, $l) {
                $id = Document::where('organization_id', $org->id)->orderBy('id')->value('id');

                return Document::find($id)?->title === "$l doc 1";
            });
            // a correlated subquery in the select list takes the organization of the outer rows
            $this->check("$l: withCount, withSum, withExists, withMax of versions", function () use ($org) {
                $docs = Document::withCount('versions')->withSum('versions', 'id')->withExists('versions')->withMax('versions', 'id')
                    ->where('organization_id', $org->id)->orderBy('id')->get();

                return $docs->count() === 3 && $docs->every(fn ($d) => (int) $d->versions_count === 2 && (bool) $d->versions_exists
                    && (int) $d->versions_sum_id === (int) $d->versions()->sum('id') && (int) $d->versions_max_id === (int) $d->versions()->max('id'));
            });
            $this->check("$l: withCount by id (no tenant in the query)", function () use ($org) {
                $id = Document::where('organization_id', $org->id)->orderBy('id')->value('id');

                return (int) Document::withCount('versions')->find($id)?->versions_count === 2;
            });
        }

        $new = null;
        $this->check('A: create document with version and comment', function () use ($a, &$new) {
            $new = Document::create([
                'organization_id' => $a->id, 'project_id' => $a->id, 'slug' => 'a-new', 'title' => 'New A', 'body' => 'secret A new',
            ]);
            $new->versions()->create(['content' => 'first draft']);
            Comment::create(['document_id' => $new->id, 'author' => 'bob', 'body' => 'hi']);
            $fresh = Document::with(['versions', 'comments'])->find($new->id);

            return $fresh->versions->count() === 1 && $fresh->comments->count() === 1;
        });
        $this->check('A: update by id', function () use ($a) {
            $doc = Document::where('organization_id', $a->id)->where('slug', 'a-2')->first();
            $doc->update(['title' => 'A doc 2 (edited)']);

            return Document::find($doc->id)->title === 'A doc 2 (edited)';
        });
        $this->check('A: transaction commit', function () use ($a) {
            DB::transaction(function () use ($a) {
                $doc = Document::create(['organization_id' => $a->id, 'project_id' => $a->id, 'slug' => 'a-tx', 'title' => 'Tx', 'body' => 'secret tx']);
                $doc->versions()->create(['content' => 'in tx']);
            });

            return Document::where('organization_id', $a->id)->where('slug', 'a-tx')->first()?->versions()->count() === 1;
        });
        $this->check('A: transaction rollback', function () use ($a) {
            try {
                DB::transaction(function () use ($a) {
                    Document::create(['organization_id' => $a->id, 'project_id' => $a->id, 'slug' => 'a-rollback', 'title' => 'Rollback']);
                    throw new \RuntimeException('boom');
                });
            } catch (\RuntimeException) {
            }

            return ! Document::where('organization_id', $a->id)->where('slug', 'a-rollback')->exists();
        });
        foreach ([[$a, 'a'], [$b, 'b']] as [$org, $l]) {
            $this->check(strtoupper($l).': upsert (update existing + insert new)', function () use ($org, $l) {
                Document::upsert([
                    ['organization_id' => $org->id, 'project_id' => $org->id, 'slug' => "$l-1", 'title' => "$l-1 upserted", 'body' => 'x'],
                    ['organization_id' => $org->id, 'project_id' => $org->id, 'slug' => "$l-upsert", 'title' => 'brand new', 'body' => 'x'],
                ], ['organization_id', 'slug'], ['title']);

                return Document::where('organization_id', $org->id)->where('slug', "$l-1")->value('title') === "$l-1 upserted"
                    && Document::where('organization_id', $org->id)->where('slug', "$l-upsert")->exists();
            });
        }
        $this->check('A: delete cascades to versions (customer FK) and comments (cloud FK)', function () use (&$new) {
            $id = $new->id;
            $new->delete();

            return Document::find($id) === null
                && DocumentVersion::where('document_id', $id)->count() === 0
                && Comment::where('document_id', $id)->count() === 0;
        });
        $this->check('A: join + group by', function () use ($a) {
            $rows = DB::table('documents')
                ->join('projects', 'projects.id', '=', 'documents.project_id')
                ->where('documents.organization_id', $a->id)
                ->groupBy('projects.name')
                ->select('projects.name', DB::raw('count(*) as c'))
                ->get();

            return $rows->count() === 1 && $rows[0]->name === 'A-Project' && (int) $rows[0]->c === 5;
        });
        $this->check('B: lookup by slug only (all tenants scanned)', function () use ($b) {
            return Document::where('slug', 'b-2')->first()?->organization_id === $b->id;
        });
    }

    private function r2(Organization $a, Organization $b): void
    {
        $this->check('A+B: new column has its default', function () use ($a, $b) {
            return Document::where('organization_id', $a->id)->where('slug', 'a-1')->value('status') === 'draft'
                && Document::where('organization_id', $b->id)->where('slug', 'b-1')->value('status') === 'draft';
        });
        $this->check('A: write and read the new column', function () use ($a) {
            $doc = Document::create(['organization_id' => $a->id, 'project_id' => $a->id, 'slug' => 'a-r2', 'title' => 'R2', 'status' => 'published']);

            return Document::find($doc->id)->status === 'published';
        });
        $this->check('B: update the new column', function () use ($b) {
            $doc = Document::where('organization_id', $b->id)->where('slug', 'b-2')->first();
            $doc->update(['status' => 'archived']);

            return Document::find($doc->id)->status === 'archived';
        });
    }

    private function offline(Organization $a, Organization $b): void
    {
        $this->check('B: works while A\'s database is down (list, find, create)', function () use ($b) {
            $docs = Document::with('versions')->where('organization_id', $b->id)->get();
            $doc = Document::find($docs->first()->id);
            $created = Document::create(['organization_id' => $b->id, 'project_id' => $b->id, 'slug' => 'b-offline-'.uniqid(), 'title' => 'while A is down']);

            return $docs->count() > 0 && $doc !== null && Document::find($created->id) !== null;
        });
        $this->check('A: fails with an error instead of falling back to the cloud', function () use ($a) {
            try {
                Document::where('organization_id', $a->id)->get();

                return 'query unexpectedly succeeded';
            } catch (\Illuminate\Database\QueryException $e) {
                $this->line('     error: '.strtok($e->getMessage(), "\n"));

                return true;
            }
        });
    }

    private function check(string $name, callable $fn): void
    {
        try {
            $ok = $fn();
        } catch (\Throwable $e) {
            $ok = get_class($e).': '.$e->getMessage();
        }
        if ($ok === true) {
            $this->line("PASS  $name");
        } else {
            $this->fails++;
            $this->line("FAIL  $name".($ok === false ? '' : ": $ok"));
        }
    }
}
