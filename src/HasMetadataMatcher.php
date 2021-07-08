<?php

/**
 * This file is part of prooph/pdo-event-store.
 * (c) 2016-2021 Alexander Miertsch <kontakt@codeliner.ws>
 * (c) 2016-2021 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventStore\Pdo;

use Prooph\EventStore\Metadata\MetadataMatcher;

/**
 * Additional interface to be implemented for persistence strategies
 * to to provide their own logic to create where clauses
 */
interface HasMetadataMatcher
{
    public function createWhereClause(?MetadataMatcher $metadataMatcher): array;
}
